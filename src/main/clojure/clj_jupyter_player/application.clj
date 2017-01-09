(ns clj-jupyter-player.application
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [clj-jupyter-player.shell :as shell]
            [clj-jupyter-player.kernel :as kernel]
            [clj-jupyter-player.util :as util]
            [clj-jupyter-player.cell :as cell])
  (:import java.util.UUID))

(defn run-notebook
  [tmp-dir shutdown-signal stdin-port iopub-port hb-port control-port shell-port transport ip secret-key notebook-file]
  (try
      (let [shell-channel (async/chan)
            shell (shell/start {:ch shell-channel
                                :stdin-port stdin-port
                                :iopub-port iopub-port
                                :hb-port hb-port
                                :control-port control-port
                                :shell-port shell-port
                                :transport transport
                                :ip ip
                                :secret-key secret-key
                                :session (str (UUID/randomUUID))} shutdown-signal)
            notebook (with-open [r (io/reader notebook-file)]
                       (json/read r))]
        (log/info "start sleep")
        (Thread/sleep 1000)
        (log/info "end sleep")
        (doseq [cell (get notebook "cells")]
          (cell/execute shell-channel cell)))
      (catch Exception e
        (log/error (util/stack-trace-to-string e)))
      (finally (util/recursive-delete-dir tmp-dir))))

(defn app
  ([tmp-dir kernel-name kernel-config-file notebook-file]
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
    (run-notebook tmp-dir shutdown-signal stdin-port iopub-port hb-port control-port shell-port transport ip secret-key notebook-file)))
  ([tmp-dir kernel-name kernel-config-file notebook-file debug-connection-file]
  (let [shutdown-signal (promise)
        connection-config (with-open [r (io/reader debug-connection-file)]
                            (json/read r))
        transport    (get connection-config "transport")
        ip           (get connection-config "ip")
        secret-key   (get connection-config "key")
        stdin-port   (get connection-config "stdin_port")
        iopub-port   (get connection-config "iopub_port")
        hb-port      (get connection-config "hb_port")
        control-port (get connection-config "control_port")
        shell-port   (get connection-config "shell_port")]
    (run-notebook tmp-dir shutdown-signal stdin-port iopub-port hb-port control-port shell-port transport ip secret-key notebook-file))))
