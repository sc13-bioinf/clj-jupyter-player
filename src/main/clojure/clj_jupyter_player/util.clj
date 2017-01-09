(ns clj-jupyter-player.util
  (:require [clojure
             [string :as string]
             [walk :as walk]]
            [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:import [java.io StringWriter PrintWriter]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; LifeCycle

(defprotocol ILifecycle
  (init [this])
  (close [this]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Data format conversion

(defn snake->kebab [string]
  (string/replace string #"_" "-"))

(defn json->edn [json-string]
  (let [f (fn [[k v]] (if (string? k) [(keyword (snake->kebab k)) v] [k v]))]
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x))
                   (json/read-str json-string))))

(defn kebab->snake [string]
  (string/replace string #"-" "_"))

(defn edn->json [edn-map]
  (json/write-str
   (let [f (fn [[k v]] (if (keyword? k) [(kebab->snake (name k)) v] [k v]))]
     (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) edn-map))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Time

(def iso-fmt (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSSS"))

(defn now []
  (.format iso-fmt (java.util.Date.)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Exceptions

(defn stack-trace-to-string
  "Returns a string containing the output of .printStackTrace"
  [t]
  (let [sw (StringWriter.)
        pw (PrintWriter. sw)
        _ (.printStackTrace t pw)]
    (.toString sw)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; TMP_DIR

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
