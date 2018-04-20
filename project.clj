(defproject clj-jupyter-player-broken "0.0.14"
  :description "A standalone player for Jupyter Notebooks."
  :url "https://github.com/sc13-bioinf/clj-jupyter-player.git"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[beckon "0.1.1"]
                 [cheshire "5.7.0"]
                 [cider/cider-nrepl "0.15.1"]
                 [clj-time "0.11.0"]
                 [com.cemerick/pomegranate "1.0.0"]
                 [com.taoensso/timbre "4.8.0"]
                 [net.cgrand/parsley "0.9.3" :exclusions [org.clojure/clojure]]
                 [net.cgrand/sjacket "0.1.1" :exclusions [org.clojure/clojure
                                                          net.cgrand.parsley]]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.codec "0.1.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/core.async "0.2.385"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [datascript "0.15.5"]
                 [pandect "0.5.4"]
                 [hiccup "1.0.5"]]
  :main clj-jupyter-player.core
  :source-paths ["src/main/clojure"]
  :profiles
  {:dev     {:source-paths ["src/dev/clojure"]
             :dependencies [[org.clojure/tools.namespace "0.2.11"
                             :exclude [org.clojure/clojure]]]}
   :uberjar {:aot :all}}
:uberjar-name "IClojurePlayer.jar")
