(ns clj-jupyter-player.application
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]))

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
  [kernel-config-file notebook-file]
  (let [tmp-dir (mk-tmp-dir :base-name "clj-jupyter-player")]
    (try
      (let [kernel :k]
        (throw (Exception. "out of cheese")))
      (catch Exception e
        (log/error (.getMessage e)))
      (finally (recursive-delete-dir tmp-dir)))))
