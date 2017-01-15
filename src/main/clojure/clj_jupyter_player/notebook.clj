(ns clj-jupyter-player.notebook
  (:require [clojure.core.async :as async]
            [clojure.string :as string]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [datascript.core :as d]
            [clj-jupyter-player.util :as util]))

(defn execute-notebook
  [conn notebook]
  (d/transact! conn [[:db/add [:db/ident :notebook] :notebook/metadata (get notebook "metadata")]
                     [:db/add [:db/ident :notebook] :notebook/nbformat (get notebook "nbformat")]
                     [:db/add [:db/ident :notebook] :notebook/nbformat-minor (get notebook "nbformat_minor")]]))

(defn execute-cell
  [conn shell-channel cell]
  (let [cell-type (get cell "cell_type")
        source (get cell "source")
        tx-result (d/transact! conn [[:db/add -1 :notebook.cell/type cell-type]
                                     [:db/add -1 :notebook.cell/empty? (empty? source)]
                                     [:db/add -1 :notebook.cell/source source]
                                     [:db/add -1 :notebook.cell/metadata (get cell "metadata")]
                                     [:db/add [:db/ident :notebook] :notebook/cells -1]])
        cell-eid (get (:tempids tx-result) -1)]
    (if (and (= cell-type "code")
             (not (empty? source)))
      (do
        (log/info "running source: " source)
        (async/>!! shell-channel {:command :send
                                  :cell-eid cell-eid
                                  :source source})))))

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
                                                   "data" (util/edn->json (:jupyter.response/data response))
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

(defn render-cell-output
  [conn cell]
  (if-let [request-eid (-> cell :notebook.cell.player/execute-request :db/id)]
    (if-let[responses (:jupyter/response (d/pull @conn '[{:jupyter/response [*]}] request-eid))]
      (let [sorted-responses (sort-responses conn request-eid responses)
            _ (log/info "sorted-responses: " sorted-responses)
            execution-count (:jupyter.response/execution-count (last (filter #(contains? % :jupyter.response/execution-count) sorted-responses)))
            _ (log/info "execution-count: " execution-count)
            output-responses (vec (remove #(nil? (some #{:jupyter.response/stream :jupyter.response/data :jupyter.response/ename} (keys %))) sorted-responses))
            _ (log/info "output-responses: " output-responses)]
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

