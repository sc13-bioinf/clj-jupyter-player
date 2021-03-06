(defproject clj-jupyter-player "0.0.13"
  :description "A standalone player for Jupyter Notebooks."
  :url "https://github.com/sc13-bioinf/clj-jupyter-player.git"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.taoensso/timbre "4.7.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/core.async "0.2.385"]
                 [datascript "0.15.5"]
                 [org.zeromq/jeromq "0.4.3"]]
  :main clj-jupyter-player.core
  :source-paths ["src/main/clojure"]
  :profiles
  {:dev     {:source-paths ["src/dev/clojure"]
             :dependencies [[org.clojure/tools.namespace "0.2.11"
                             :exclude [org.clojure/clojure]]]}
   :uberjar {:aot :all}}
:uberjar-name "IClojurePlayer.jar")
