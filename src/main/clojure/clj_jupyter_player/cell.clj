(ns clj-jupyter-player.cell
  (:require [clojure.core.async :as async]
            [taoensso.timbre :as log]))

(defn execute
  [shell-channel cell]
  (let [cell-type (get cell "cell_type")
        source (get cell "source")]
    (if (and (= cell-type "code")
             (not (empty? source)))
      (do
        (log/info "running source: " source)
        (async/>!! shell-channel {:command :send
                                  :source source})))))
