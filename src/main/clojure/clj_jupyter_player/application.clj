(ns clj-jupyter-player.application
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [datascript.core :as d]
            [clojure.tools.nrepl :as nrepl]
            [clojure.tools.nrepl.server :as nrepl.server]
            [clojupyter.middleware.mime-values]
            [clj-jupyter-player.util :as util]
            [clj-jupyter-player.notebook :as notebook])
  (:import java.util.UUID))

(def clojupyter-middleware
  '[clojupyter.middleware.mime-values/mime-values])

(defn clojupyter-nrepl-handler []
  ;; dynamically load to allow cider-jack-in to work
  ;; see https://github.com/clojure-emacs/cider-nrepl/issues/447
  (require 'cider.nrepl)
  (apply nrepl.server/default-handler
         (map resolve
              (concat (var-get (ns-resolve 'cider.nrepl 'cider-middleware))
                      clojupyter-middleware))))

(defn start-nrepl-server []
  (nrepl.server/start-server
   :handler (clojupyter-nrepl-handler)))

(defn run-notebook
  [tmp-dir notebook-file notebook-output-file preload-notebook-file update-preload-index]
  (with-open [nrepl-server    (start-nrepl-server)
              nrepl-transport (nrepl/connect :port (:port nrepl-server))]
    (let [nrepl-client  (nrepl/client nrepl-transport Integer/MAX_VALUE)
          nrepl-session (nrepl/new-session nrepl-client)
          schema {:db/ident {:db/unique :db.unique/identity}
                  :jupyter/msg-id {:db/unique :db.unique/identity}
                  :jupyter/response {:db/cardinality :db.cardinality/many
                                     :db/valueType   :db.type/ref}
                  :notebook/cells {:db/cardinality :db.cardinality/many
                                   :db/valueType   :db.type/ref}
                  :notebook.cell.player/execute-request {:db/valueType :db.type/ref}}
          datoms [(d/datom 1 :db/ident :notebook)
                  (d/datom 1 :notebook/loaded false)]
          conn (d/conn-from-db (d/init-db datoms schema))
          notebook (with-open [r (io/reader notebook-file)]
                     (json/read r))]
      (let [cells (notebook/splice-cells (get notebook "cells") preload-notebook-file update-preload-index)
            execution-counter (atom 0)]
        (if (nil? cells)
          (do
            (log/error "Failed to run notebook, invalid cells")
            1)
          (do
            (notebook/execute-notebook conn notebook)
            (doseq [cell cells]
              (notebook/execute-cell conn cell nrepl-client nrepl-session execution-counter))
            (notebook/execute-loaded conn)
            (with-open [w (io/writer notebook-output-file)]
              (json/write (notebook/render conn notebook) w))
            0))))))

(defn app
  ([tmp-dir notebook-file notebook-output-file preload-notebook-file update-preload-index]
  (let [exit-code (try
                    (when-let [shell (run-notebook tmp-dir notebook-file notebook-output-file preload-notebook-file update-preload-index)]
                      (log/info "Completed notebook")
                      shell)
                    (catch Exception e
                      (log/error (util/stack-trace-to-string e))
                      1)
                    (finally (util/recursive-delete-dir tmp-dir)))]
    (shutdown-agents)
    exit-code))
  ([tmp-dir notebook-file notebook-output-file preload-notebook-file update-preload-index debug-connection-file]
  (let [connection-config (with-open [r (io/reader debug-connection-file)]
                            (json/read r))
        exit-code (try
                    (when-let [shell (run-notebook tmp-dir notebook-file notebook-output-file preload-notebook-file update-preload-index)]
                      (log/info "Completed notebook"))
                    (catch Exception e
                      (log/error (util/stack-trace-to-string e))
                      1)
                    (finally (util/recursive-delete-dir tmp-dir)))]
    (shutdown-agents)
    exit-code)))
