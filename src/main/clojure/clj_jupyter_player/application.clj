(ns clj-jupyter-player.application
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [datascript.core :as d]
            [clj-jupyter-player.shell :as shell]
            [clj-jupyter-player.kernel :as kernel]
            [clj-jupyter-player.util :as util]
            [clj-jupyter-player.notebook :as notebook])
  (:import java.util.UUID))

(defn responses-complete?
  [conn request-eid responses]
  (let [response-order (into {}
                             (map-indexed #(vector %2 %1)
                                          (->> (d/datoms @conn :aevt :jupyter/response request-eid)
                                               (sort-by :tx)
                                               (map :v))))
        sorted-responses (sort-by #(get response-order (:db/id %)) responses)
        last-execution-state (last (remove nil? (map :jupyter.response/execution-state sorted-responses)))
        response-status (remove nil? (map :jupyter.response/status sorted-responses))]
    (and (= last-execution-state "idle")
         (every? #(= "ok" %) response-status))))

(defn cell-completed?
  "Do we have all of the output from this cell?"
  [conn cell]
  (if (or (not= (:notebook.cell/type cell) "code")
          (and (= (:notebook.cell/type cell) "code")
               (:notebook.cell/empty? cell)))
    true
    (if-let [request-eid (-> cell :notebook.cell.player/execute-request :db/id)]
      (if-let[responses (:jupyter/response (d/pull @conn '[{:jupyter/response [*]}] request-eid))]
          (responses-complete? conn request-eid responses)
          false)
      false)))

(defn notebook-completed?
  "Send a message on the channel when all cells are finished"
  [conn notebook-channel tx-report]
  (let [cells (:notebook/cells (d/pull @conn '[{:notebook/cells [:notebook.cell/type :notebook.cell/empty? :notebook.cell.player/execute-request]}] [:db/ident :notebook]))
        _ (log/info "notebook-completed? cells: " (vec (map (partial cell-completed? conn) cells)))]
    (when (every? (partial cell-completed? conn) cells)
      (log/info "send notebook-done")
      (async/>!! notebook-channel :notebook-done))))

(defn kernel-shutdown?
  "Send a message on the channel when the kernel has shutdown"
  [conn notebook-channel tx-report]
  (let [kernel-shutdown (first (filter :notebook.player/shutdown-request
                                       (d/q '[:find [(pull ?e [:db/id :notebook.player/shutdown-request {:jupyter/response [*]}])]
                                                               :where [?e :notebook.player/shutdown-request true]]
                                            @conn)))
        _ (log/info "kernel-shutdown: " kernel-shutdown)]
    (if-let [responses (:jupyter/response kernel-shutdown)]
      (if (responses-complete? conn (:db/id kernel-shutdown) responses)
        (do
          (log/info "kernel-shutdown? is complete: " responses)
          (async/>!! notebook-channel :kernel-shutdown))
        (log/info "kernel-shutdown? incomplete: " responses)))))

(defn run-notebook
  [tmp-dir stdin-port iopub-port hb-port control-port shell-port transport ip secret-key notebook-file notebook-output-file]
  (try
      (let [schema {:db/ident {:db/unique      :db.unique/identity}
                    :jupyter/msg-id {:db/unique      :db.unique/identity}
                    :jupyter/response  {:db/cardinality :db.cardinality/many
                                        :db/valueType   :db.type/ref}
                    :notebook/cells    {:db/cardinality :db.cardinality/many
                                        :db/valueType   :db.type/ref}
                    :notebook.cell.player/execute-request {:db/valueType   :db.type/ref}}
            datoms [(d/datom 1 :db/ident :notebook)]
            conn (d/conn-from-db (d/init-db datoms schema))
            notebook-channel (async/chan)
            _ (d/listen! conn :notebook-done (partial notebook-completed? conn notebook-channel))
            shell-channel (async/chan)
            shell (shell/start {:conn conn
                                :ch shell-channel
                                :stdin-port stdin-port
                                :iopub-port iopub-port
                                :hb-port hb-port
                                :control-port control-port
                                :shell-port shell-port
                                :transport transport
                                :ip ip
                                :secret-key secret-key
                                :session (str (UUID/randomUUID))})
            notebook (with-open [r (io/reader notebook-file)]
                       (json/read r))]
        (async/go-loop [msg (async/<! notebook-channel)]
          (cond
            (= msg :notebook-done) (do
                                     (d/unlisten! conn :notebook-done)
                                     (d/listen! conn :kernel-shutdown (partial kernel-shutdown? conn notebook-channel))
                                     (with-open [w (io/writer notebook-output-file)]
                                       (json/write (notebook/render conn notebook) w))
                                     (async/>! shell-channel {:command :shutdown})
                                     (recur (async/<! notebook-channel)))
            (= msg :kernel-shutdown) (do
                                       (d/unlisten! conn :kernel-shutdown)
                                       (async/>! shell-channel {:command :stop})
                                       (log/info "shutdown notebook listener"))
            :else (do
                    (log/error "Say what? Don't understand notebook-channel msg: " msg)
                    (recur (async/<! notebook-channel)))))
        (log/info "start sleep")
        (Thread/sleep 1000)
        (log/info "end sleep")
        (doseq [cell (get notebook "cells")]
          (notebook/execute-cell conn shell-channel cell)))
      (catch Exception e
        (log/error (util/stack-trace-to-string e)))
      (finally (util/recursive-delete-dir tmp-dir))))

(defn app
  ([tmp-dir kernel-name kernel-config-file notebook-file notebook-output-file]
  (let [shutdown-signal (promise)
        transport "tcp"
        ip "127.0.0.1"
        secret-key (str (UUID/randomUUID))
        ports (shell/reserve-ports 5)
        port-order (-> ports keys vec)
        stdin-port   (shell/release-port ports (get port-order 0))
        iopub-port   (shell/release-port ports (get port-order 1))
        hb-port      (shell/release-port ports (get port-order 2))
        control-port (shell/release-port ports (get port-order 3))
        shell-port   (shell/release-port ports (get port-order 4))
        connection-config {"transport" transport
                           "kernel_name" kernel-name
                           "ip" ip
                           "key" secret-key
                           "signature_scheme" "hmac-sha256"
                           "stdin_port" stdin-port
                           "iopub_port" iopub-port
                           "hb_port"    hb-port
                           "control_port" control-port
                           "shell_port" shell-port}
        connection-file (io/file tmp-dir "connection.json")
        _ (with-open [w (io/writer connection-file)]
            (json/write connection-config w))
        kernel-config (with-open [r (io/reader kernel-config-file)]
                        (json/read r))
        kernel (kernel/start {:kernel-config kernel-config
                              :connection-file connection-file
                              :tmp-dir tmp-dir} shutdown-signal)]
    (run-notebook tmp-dir stdin-port iopub-port hb-port control-port shell-port transport ip secret-key notebook-file notebook-output-file)))
  ([tmp-dir kernel-name kernel-config-file notebook-file notebook-output-file debug-connection-file]
  (let [connection-config (with-open [r (io/reader debug-connection-file)]
                            (json/read r))
        transport    (get connection-config "transport")
        ip           (get connection-config "ip")
        secret-key   (get connection-config "key")
        stdin-port   (get connection-config "stdin_port")
        iopub-port   (get connection-config "iopub_port")
        hb-port      (get connection-config "hb_port")
        control-port (get connection-config "control_port")
        shell-port   (get connection-config "shell_port")]
    (run-notebook tmp-dir stdin-port iopub-port hb-port control-port shell-port transport ip secret-key notebook-file notebook-output-file))))
