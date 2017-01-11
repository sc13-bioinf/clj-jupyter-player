(ns clj-jupyter-player.cell
  (:require [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [datascript.core :as d]
            [clj-jupyter-player.util :as util]))

(defn execute
  [conn shell-channel cell]
  (let [cell-type (get cell "cell_type")
        source (get cell "source")
        tx-result (d/transact! conn [[:db/add -1 :notebook.cell/type cell-type]
                                     [:db/add -1 :notebook.cell/empty? (empty? source)]
                                     [:db/add [:db/ident :notebook] :notebook/cells -1]])
        cell-eid (get (:tempids tx-result) -1)]
    (if (and (= cell-type "code")
             (not (empty? source)))
      (do
        (log/info "running source: " source)
        (async/>!! shell-channel {:command :send
                                  :cell-eid cell-eid
                                  :source source})))))
