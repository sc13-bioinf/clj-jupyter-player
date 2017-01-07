(ns clj-jupyter-player.process
  (:require [taoensso.timbre :as log])
  (:import [java.lang ProcessBuilder]))

(defn run
  [run-dir params]
  (let [pb (ProcessBuilder. params)
        _ (.directory pb run-dir)
        _ (.redirectErrorStream pb true)
        p (.start pb)]
    p))
