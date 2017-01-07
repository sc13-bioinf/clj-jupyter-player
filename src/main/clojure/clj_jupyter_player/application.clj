(ns clj-jupyter-player.application
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [clj-jupyter-player.shell :as shell]
            [clj-jupyter-player.kernel :as kernel]
            [clj-jupyter-player.util :as util])
  (:import java.util.UUID))

(defn mk-tmp-dir
  "Create a tmp dir, by default based on java.io.tmpdir"
  [& {:keys [base-dir base-name unique] :or {base-dir (System/getProperty "java.io.tmpdir")
                                                     base-name ""
                                                     unique (str "-" (System/currentTimeMillis) "-" (long (rand 1000000000)) "-")}}]
  (let [max-attempts 100]
    (loop [num-attempts 1]
      (if (= num-attempts max-attempts)
        (throw (Exception. (str "Failed to create temporary directory after " max-attempts " attempts.")))
        (let [tmp-base (clojure.string/join (System/getProperty "file.separator") [base-dir (str base-name unique)])
              tmp-dir-name (if (empty? unique) tmp-base (str tmp-base num-attempts))
              tmp-dir (io/file tmp-dir-name)]
          (log/info "mk-tmp-dir: " tmp-dir)
              (if (.mkdirs tmp-dir)
                tmp-dir
                (recur (inc num-attempts))))))))

(defn recursive-delete-dir
  "Recursively delete a directory"
  [file]
  (let [files (file-seq file)
        first-path (.toPath (first files))
        _ (when (and (.isDirectory (first files))
                     (= "/" (-> first-path (.normalize) (.toString))))
            (throw (Exception. "Attemped to delete '/'")))
        _ (when (and (.isDirectory (first files))
                     (not (nil? (.getParent first-path)))
                     (= "/" (-> first-path (.getParent) (.normalize) (.toString))))
            (throw (Exception. "Attempted to delete '/[DIR]'")))]
    (doseq [f (reverse files)]
      (io/delete-file f))))

(defn app
  [kernel-name kernel-config-file notebook-file]
  (let [tmp-dir (mk-tmp-dir :base-name "clj-jupyter-player")
        connection-file (io/file tmp-dir "connection.json")
        shutdown-signal (promise)
        transport "tcp"
        ip "127.0.0.1"
        secret-key (str (UUID/randomUUID))
        ports (shell/reserve-ports 5)
        port-order (-> ports keys vec)]
    (try
      (let [shell (shell/start {:ports ports
                                :port-order port-order
                                :transport transport
                                :ip ip
                                :key secret-key} shutdown-signal)
            connection-config {"transport" transport
                               "kernel-name" kernel-name
                               "ip" ip
                               "key" secret-key
                               "signature-scheme" "hmac-sha256"
                               "stdin-port" (get port-order 0)
                               "iopub-port" (get port-order 1)
                               "hb-port" (get port-order 2)
                               "control-port" (get port-order 3)
                               "shell-port" (get port-order 4)
                               }
            _ (with-open [w (io/writer connection-file)]
                (json/write connection-config w))
            kernel-config (with-open [r (io/reader kernel-config-file)]
                            (json/read r))
            kernel (kernel/start {:kernel-config kernel-config
                                  :connection-file connection-file
                                  :tmp-dir tmp-dir} shutdown-signal)
            _ (log/info "got started kernel: " kernel)
            notebook (with-open [r (io/reader notebook-file)]
                       (json/read r))]
        (log/info "start sleep")
        (Thread/sleep 20000)
        (doseq [cell (get notebook "cells")]
          (log/info "cell: " cell))
        )
      (catch Exception e
        (log/error (util/stack-trace-to-string e)))
      (finally (recursive-delete-dir tmp-dir)))))
