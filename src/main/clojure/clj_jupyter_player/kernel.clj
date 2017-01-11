(ns clj-jupyter-player.kernel
  (:require [taoensso.timbre :as log]
            [clj-jupyter-player.process :as process]
            [clj-jupyter-player.util :as util]))

(defn update-with-connection-file
  [argv connection-file]
  (let [connection-file-index (first (keep-indexed #(when (= %2 "{connection_file}") %1) argv))
        _ (when (nil? connection-file-index) (throw (Exception. "Kernel config argv does not contain '{connection_file}'")))]
    (assoc-in argv [connection-file-index] (.getPath connection-file))))

(defrecord KernelSystem [config]
  util/ILifecycle
  (init [{{{:strs [display_name argv language]} :kernel-config
           :keys [connection-file tmp-dir]} :config
          :as this}]
    (let [argv-with-connection (update-with-connection-file argv connection-file)
          _ (log/info "trying to run kernel with tmp-dir: " tmp-dir)
          kernel-process (process/run tmp-dir argv-with-connection)
          _ (log/info "kernel-process: " kernel-process)]
      (assoc this :process kernel-process)))
  (close [{:keys [process]}]
    (log/info "Kernel closed.")))

(defn create-kernel [config]
  (util/init (->KernelSystem config)))

(defn start
  [config]
  (log/info "Starting kernel ...")
  (with-open [^KernelSystem kernel-system (create-kernel config)]
    (with-open [k-rdr (clojure.java.io/reader (.getInputStream (:process kernel-system)))]
      (doseq [line  (line-seq k-rdr)]
        (log/info "kernel-line: " line)))))


