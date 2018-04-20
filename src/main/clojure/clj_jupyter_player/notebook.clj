(ns clj-jupyter-player.notebook
  (:require [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [datascript.core :as d]
            [clojure.tools.nrepl :as nrepl]
            [clj-jupyter-player.util :as util]
            [clojure.edn :as edn])
  (:import [java.util Date UUID]))

(defn stacktrace-string
  "Return a nicely formatted string."
  [msg]
  (when-let [st (:stacktrace msg)]
    (let [clean (->> st
                     (filter (fn [f] (not-any? #(= "dup" %) (:flags f))))
                     (filter (fn [f] (not-any? #(= "tooling" %) (:flags f))))
                     (filter (fn [f] (not-any? #(= "repl" %) (:flags f))))
                     (filter :file))
          max-file (apply max (map count (map :file clean)))
          max-name (apply max (map count (map :name clean)))]
      (map #(format (str "%" max-file "s: %5d %-" max-name "s")
                    (:file %) (:line %) (:name %))
           clean))))

(defn execute-notebook
  [conn notebook]
  (d/transact! conn [[:db/add [:db/ident :notebook] :notebook/metadata (get notebook "metadata")]
                     [:db/add [:db/ident :notebook] :notebook/nbformat (get notebook "nbformat")]
                     [:db/add [:db/ident :notebook] :notebook/nbformat-minor (get notebook "nbformat_minor")]]))

(defn execute-cell
  [conn cell nrepl-client nrepl-session exec-counter]
  (let [pending (atom #{})
        output-texts (atom [])
        result (atom {})
        cell-type (get cell "cell_type")
        source (get cell "source")
        tx-result (d/transact! conn [[:db/add -1 :notebook.cell/type cell-type]
                                     [:db/add -1 :notebook.cell/empty? (empty? source)]
                                     [:db/add -1 :notebook.cell/source source]
                                     [:db/add -1 :notebook.cell/metadata (get cell "metadata")]
                                     [:db/add [:db/ident :notebook] :notebook/cells -1]])
        cell-eid (get (:tempids tx-result) -1)
        done? (fn [{:keys [id status] :as msg} pending]
                (let [pending? (@pending id)]
                  (swap! pending disj id)
                    (and pending? (some #{"interrupted" "done" "error"} status))))]
    (when (and (= cell-type "code")
             (not (empty? source)))
        (let [msg-id (UUID/randomUUID)
              _ (log/debug {:command :send
                            :cell-eid cell-eid
                            :source source})
              _ (d/transact! conn [[:db/add -1 :jupyter/msg-id msg-id]
                                   [:db/add -1 :jupyter.player/sent (Date.)]
                                   [:db/add cell-eid :notebook.cell.player/execute-request -1]])
              _ (swap! pending conj cell-eid)
              _ (swap! exec-counter inc)]
          (doseq [{:keys [ns out err status session ex value] :as msg}
            (nrepl/message nrepl-client {:id cell-eid
                                             :op "eval"
                                             :code (str (string/join "" source) "\n")
                                             :session nrepl-session})
                :while (not (done? msg pending))]
            (log/debug "nrepl result: " msg)
            (when-not (nil? ex)
              (swap! result assoc :ename ex))
            (when-not (nil? out)
              (swap! output-texts conj out))
            (when-not (nil? err)
              (swap! output-texts conj err))
            (when-not (nil? value)
              (d/transact! conn (conj (vec (util/tx-data-from-map -1 {:jupyter.response/execution-count @exec-counter
                                                                      :jupyter.response/data (if (= value "nil") nil (json/read-str value))
                                                                      :jupyter.response/metadata {}
                                                                   }))
                                      [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]))))
         (when-not (empty? @output-texts)
            (d/transact! conn [[:db/add -1 :jupyter.response/stream {"name" "stdout"
                                                                     "text" @output-texts}]
                               [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]]))
          (when (contains? @result :ename)
            (let [traceback (if (re-find #"StackOverflowError" (:ename @result))
                              []
                              (stacktrace-string (-> nrepl-client
                                                     (nrepl/message {:op :stacktrace
                                                                     :session nrepl-session})
                                                     nrepl/combine-responses
                                                     doall)))]
              (d/transact! conn (conj (vec (util/tx-data-from-map -1 {:jupyter.response/execution-count @exec-counter
                                                                      :jupyter.response/ename (:ename @result)
                                                                      :jupyter.response/evalue ""
                                                                      :jupyter.response/traceback traceback}))
                                      [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]))))))))

(defn execute-loaded
  [conn]
  (d/transact! conn [[:db/add [:db/ident :notebook] :notebook/loaded true]]))

(defn sort-responses
  [conn request-eid responses]
  (let [response-order (into {}
                             (map-indexed #(vector %2 %1)
                                          (->> (d/datoms @conn :aevt :jupyter/response request-eid)
                                               (sort-by :tx)
                                               (map :v))))]
    (sort-by #(get response-order (:db/id %)) responses)))

(defn render-output-responses
  [output-responses]
  (for [response output-responses]
    (cond
      (contains? response :jupyter.response/stream) (assoc (:jupyter.response/stream response) "output_type" "stream")
      (contains? response :jupyter.response/data) {"output_type" "display_data"
                                                   "data" (:jupyter.response/data response);;(util/edn->json (:jupyter.response/data response))
                                                   "metadata" (:jupyter.response/metadata response)}
      (contains? response :jupyter.response/ename) {"output_type" "error"
                                                    "ename" (:jupyter.response/ename response)
                                                    "evalue" (:jupyter.response/evalue response)
                                                    "traceback" (:jupyter.response/traceback response)}
      :else (throw (Exception. (str "Could not find known response type in " response))))))

(defn render-cell-default
  [cell]
  {"cell_type" (:notebook.cell/type cell)
   "source" (:notebook.cell/source cell)
   "metadata" (:notebook.cell/metadata cell)})

(defn drop-text-plain
  [accumulator item]
  (if (-> item :jupyter.response/data (contains? "text/plain"))
    (if (:seen accumulator)
      accumulator
      (-> accumulator
          (assoc :seen true)
          (update :result conj item)))
    (update accumulator :result conj item)))

(defn render-cell-output
  [conn cell]
  (if-let [request-eid (-> cell :notebook.cell.player/execute-request :db/id)]
    (if-let[responses (:jupyter/response (d/pull @conn '[{:jupyter/response [*]}] request-eid))]
      (let [sorted-responses (sort-responses conn request-eid responses)
            _ (log/debug "sorted-responses: " sorted-responses)
            filtered-responses (reverse (:result (reduce drop-text-plain {:seen false :result []} (reverse sorted-responses))))
            execution-count (:jupyter.response/execution-count (last (filter #(contains? % :jupyter.response/execution-count) filtered-responses)))
            _ (log/debug "execution-count: " execution-count)
            output-responses (vec (remove #(nil? (some #{:jupyter.response/stream :jupyter.response/data :jupyter.response/ename} (keys %))) filtered-responses))]
        (assoc (render-cell-default cell) "execution_count" execution-count
                                          "outputs" (render-output-responses output-responses)))
      (assoc (render-cell-default cell) "execution_count" nil "outputs" []))
    (assoc (render-cell-default cell) "execution_count" nil "outputs" [])))

(defn render-cell
  [conn cell]
  (condp = (:notebook.cell/type cell)
    "code"     (render-cell-output conn cell)
    "markdown" (render-cell-default cell)
    (throw (Exception. (str "Cell type " (:notebook.cell/type cell) " not implemented yet")))))

(defn render
  "Render the notebook into a datastructre ready for serialisation"
  [conn notebook]
  (let [notebook (d/pull @conn '[:notebook/metadata
                                 :notebook/nbformat
                                 :notebook/nbformat-minor
                                 {:notebook/cells [:notebook.cell/type :notebook.cell/source :notebook.cell/metadata :notebook.cell.player/execute-request]}]
                         [:db/ident :notebook])]
    {"metadata" (:notebook/metadata notebook)
     "nbformat" (:notebook/nbformat notebook)
     "nbformat_minor" (:notebook/nbformat-minor notebook)
     "cells" (for [cell (:notebook/cells notebook)]
               (render-cell conn cell))}))

(defn splice-cells
  "Update cells at the given index with cells from the preload notebook"
  [notebook-cells preload-notebook-file update-preload-index]
  (if (nil? preload-notebook-file)
    notebook-cells
    (if (and (>= update-preload-index 0)
             (< update-preload-index (count notebook-cells)))
      (let [preload-notebook (with-open [r (io/reader preload-notebook-file)]
                               (json/read r))
            [notebook-before notebook-after] (split-at update-preload-index notebook-cells)]
        (if (every? #(and (contains? % "cell_type")
                          (contains? % "metadata")
                          (contains? % "source")) (get preload-notebook "cells"))
          (concat notebook-before (get preload-notebook "cells") notebook-after)
          (do
            (log/error "All preload-notebook cells must contain 'cell_type', 'metadata' and 'source'")
            nil)))
      (do
        (log/error (string/join "" ["update-preload-index '" update-preload-index "' out of range 0-" (count notebook-cells)]))
      nil))))

