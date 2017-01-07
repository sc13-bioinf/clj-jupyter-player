(ns clj-jupyter-player.application
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [clj-jupyter-player.shell :as shell])
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
        secret-key (str (UUID/randomUUID))]
    (try
      (let [shell (shell/start {:transport transport :ip ip :key secret-key} shutdown-signal)
            connection-config {"transport" transport
                               "kernel-name" kernel-name
                               "stdin-port" 40681
                               "ip" ip
                               "iopub-port" (:iopub-port shell)
                               "key" secret-key
                               "hb-port" 43090
                               "signature-scheme" "hmac-sha256"
                               "control-port" 38939
                               "shell-port" 57154}
            _ (with-open [w (io/writer connection-file)]
                (json/write connection-config w))]
        (log/info "start sleep")
        (Thread/sleep 20000)
        )
      (catch Exception e
        (log/error (.getMessage e)))
      (finally (recursive-delete-dir tmp-dir)))))
