(ns clj-jupyter-player.core
  (:require [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [clj-jupyter-player.application :as application]
            [clj-jupyter-player.util :as util])
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
    :validate [#(.exists %) "File not found"]]
   ["-o" "--notebook-output-path NOTEBOOK_OUTPUT_PATH" "The output notebook file"
    :id :notebook-output-path
    :parse-fn #(io/as-file %)]
   ["-d" "--debug-connection-path DEBUG_CONNECTION_PATH" "Use an existing connection file to conenct to an already running kernel"
    :id :debug-connection-path
    :parse-fn #(io/as-file %)
    :validate [#(.exists %) "File not found"]]])

(defn -main [& args]
  (let [opts (parse-opts args cli-options)
        options (:options opts)
        kernel-name "random-kernel-name"
        tmp-dir (util/mk-tmp-dir :base-name "clj-jupyter-player")
        _ (log/info "tmp-dir: " tmp-dir)]
    (cond
      (:errors opts) (log/error (:errors opts))
      (:help options) (usage "" (:summary opts))
      (nil? (:kernel-config-path options)) (usage "Please supply kernel config path" (:summary opts))
      (nil? (:notebook-path options)) (usage "Please supply notebook path" (:summary opts))
      (nil? (:notebook-output-path options)) (usage "Please supply notebook output path" (:summary opts))
      (and (contains? options :kernel-config-path)
           (contains? options :notebook-path)
           (contains? options :notebook-output-path)) (if (contains? options :debug-connection-path)
                                                        (application/app tmp-dir
                                                                         kernel-name
                                                                         (:kernel-config-path options)
                                                                         (:notebook-path options)
                                                                         (:notebook-output-path options)
                                                                         (:debug-connection-path options))
                                                        (application/app tmp-dir
                                                                         kernel-name
                                                                         (:kernel-config-path options)
                                                                         (:notebook-path options)
                                                                         (:notebook-output-path options)))
      :else (usage "" (:summary opts)))))
