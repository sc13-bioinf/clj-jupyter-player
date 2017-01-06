(ns clj-jupyter-player.core
  (:require [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [clj-jupyter-player.application :as application])
  (:gen-class))

(defn usage
  "Print usage mesage"
  [message opt-summary]
  (println (str/join \newline [message
                               ""
                               "Usage:"
                               "\tPlease supply a kernel config path and a notebook path"
                               opt-summary
                               ])))

(def cli-options
  [["-h" "--help"]
   ["-k" "--kernel-config-path KERNEL_CONFIG_PATH" "File containing the kernel config"
    :id :kernel-config-path
    :parse-fn #(io/as-file %)
    :validate [#(.exists %) "File not found"]]
   ["-n" "--notebook-path NOTEBOOK_PATH" "The notebook file"
    :id :notebook-path
    :parse-fn #(io/as-file %)
    :validate [#(.exists %) "File not found"]]])

(defn -main [& args]
  (let [opts (parse-opts args cli-options)
        options (:options opts)]
    (cond
      (:errors opts) (log/error (:errors opts))
      (:help options) (usage "" (:summary opts))
      (nil? (:kernel-config-path options)) (usage "Please supply kernel config path" (:summary opts))
      (nil? (:notebook-path options)) (usage "Please supply notebook path" (:summary opts))
      (and (contains? options :kernel-config-path)
           (contains? options :notebook-path)) (application/app (:kernel-config-path options)
                                                                (:notebook-path options))
      :else (usage "" (:summary opts)))))

;;(defn -main [config-path notebook-path]
;;  (log/info (str/join "" ["Playing notebook '" notebook-path "' with kernel config '" config-path "'"])))
