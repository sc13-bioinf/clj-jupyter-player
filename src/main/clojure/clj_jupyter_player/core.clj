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
                               "\tPlease supply kernel config path, notebook path and notebook output path"
                               opt-summary
                               ]))
  64)

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
   ["-d" "--debug-connection-path DEBUG_CONNECTION_PATH" "Use an existing connection file to connect to an already running kernel"
    :id :debug-connection-path
    :parse-fn #(io/as-file %)
    :validate [#(.exists %) "File not found"]]
   ["-p" "--preload-notebook-path PRELOAD_NOTEBOOK_PATH" "Cells from this notebook will be injected at the index specified with -u"
    :id :preload-notebook-path
    :parse-fn #(io/as-file %)
    :validate [#(.exists %) "File not found"]]
   ["-u" "--update-preload-at-index UPDATE_PRELOAD_AT_INDEX" "Index in the list of cells where the preload notebook will be placed"
    :id :update-preload-at-index
    :parse-fn #(try (Integer. (re-find  #"\d+" %)) (catch NumberFormatException nfe nil))
    :validate [number? "Preload update index must be an integer"]]
   ["-e" "--extra-logging"]])

(defn -main [& args]
  (let [opts (parse-opts args cli-options)
        options (:options opts)
        _ (when-not (:extra-logging options) (log/set-level! :info))
        exit-code (cond
                    (:errors opts) (doseq [e (:errors opts)] (.println *err* e))
                    (:help options) (usage "" (:summary opts))
                    (nil? (:kernel-config-path options)) (usage "Please supply kernel config path" (:summary opts))
                    (nil? (:notebook-path options)) (usage "Please supply notebook path" (:summary opts))
                    (nil? (:notebook-output-path options)) (usage "Please supply notebook output path" (:summary opts))
                    (= (count (into #{} [(nil? (:preload-notebook-path options))
                                         (nil? (:update-preload-at-index options))])) 2) (usage "You must supply preload notebook path with update preload index" (:summary opts))
                    (and (contains? options :kernel-config-path)
                         (contains? options :notebook-path)
                         (contains? options :notebook-output-path)) (let [tmp-dir (util/mk-tmp-dir :base-name "clj-jupyter-player")
                                                                          _ (log/info "tmp-dir: " tmp-dir)]
                                                                      (if (contains? options :debug-connection-path)
                                                                        (application/app tmp-dir
                                                                                         (:kernel-config-path options)
                                                                                         (:notebook-path options)
                                                                                         (:notebook-output-path options)
                                                                                         (:preload-notebook-path options)
                                                                                         (:update-preload-at-index options)
                                                                                         (:debug-connection-path options))
                                                                        (application/app tmp-dir
                                                                                         (:kernel-config-path options)
                                                                                         (:notebook-path options)
                                                                                         (:notebook-output-path options)
                                                                                         (:preload-notebook-path options)
                                                                                         (:update-preload-at-index options))))
                    :else (usage "" (:summary opts)))]
    (System/exit exit-code)))
