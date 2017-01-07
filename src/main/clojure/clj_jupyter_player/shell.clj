(ns clj-jupyter-player.shell
  (:require [clojure.pprint :as pprint]
            [clj-jupyter-player.util :as util]
            [taoensso.timbre :as log])
  (:import java.net.ServerSocket
           java.io.Closeable
           javax.crypto.Mac
           javax.crypto.spec.SecretKeySpec
           [zmq Msg SocketBase ZMQ Utils]))

(set! *warn-on-reflection* true)

(defprotocol ILifecycle
  (init [this])
  (close [this]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Security

(defn signer-fn [^String key]
  (let [hmac-sha256 (Mac/getInstance "HmacSHA256")
        key         (SecretKeySpec. (.getBytes key) "HmacSHA256")]
    (.init hmac-sha256 key)
    (fn [string-list]
      (transduce (map (partial format "%02x")) str
                 (let [auth ^Mac (.clone hmac-sha256)]
                   (loop [[s & r] string-list]
                     (let [bytes (.getBytes ^String s "ascii")]
                       (if (seq r)
                         (do (.update auth bytes) (recur r))
                         (.doFinal auth bytes)))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Sockets

(defn reserve-port
  "We obtain a free local port"
  [^ServerSocket tmp-socket]
  [(.getLocalPort tmp-socket) tmp-socket])

(defn reserve-ports
  "Returns a map of temporary sockets that are reserving a local port. It is the callers responsibility to close them."
  [n]
  (into {} (map reserve-port) (repeatedly n #(ServerSocket. 0))))

(defn release-port
  [ports port]
  (.close ^java.io.Closeable (get ports port))
  port)

(defrecord SocketSystem [config]
  ILifecycle
  (init [{{:keys [ports port-order transport ip]} :config
           :as                                    this}]
    (let [ctx          (ZMQ/createContext)
          shell-socket (ZMQ/socket ctx ZMQ/ZMQ_ROUTER)
          iopub-socket (ZMQ/socket ctx ZMQ/ZMQ_PUB)
          addr         (partial str transport "://" ip ":")
          shell-port (release-port ports (get port-order 4))
          _ (ZMQ/bind shell-socket (addr shell-port))
          _ (ZMQ/setSocketOption shell-socket ZMQ/ZMQ_RCVTIMEO (int 250))
          iopub-port (release-port ports (get port-order 1))
          _ (ZMQ/bind iopub-socket (addr iopub-port))]
      (assoc this
             :ctx ctx
             :ports ports
             :shell-socket shell-socket
             :iopub-socket iopub-socket)))
  (close [{:keys [ctx ports] :as this}]
    (doseq [socket (vals (dissoc this :ctx :config :ports))]
      (ZMQ/close socket))
    (ZMQ/term ctx)
    (doseq [port (keys ports)]
      (release-port ports port))
    (log/info "All shell sockets closed.")))

(defn create-sockets [config]
  (init (->SocketSystem config)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Messaging

(def DELIM "<IDS|MSG>")

;;; Send

(defn map->blobs [{:keys [identities] :as message} signer]
  (let [data-blobs (map (comp util/edn->json message)
                        [:header :parent-header :metadata :content])
        signature  (signer data-blobs)]
    (concat identities [DELIM signature] data-blobs)))

(defn send-message [^SocketBase socket message signer]
  (loop [[^String msg & r :as l] (map->blobs message signer)]
    (log/debug "Sending " msg)
    (if (seq r)
      (do (ZMQ/send socket msg (+ ZMQ/ZMQ_SNDMORE ZMQ/ZMQ_DONTWAIT))
          (recur r))
      (ZMQ/send socket msg 0))))

;;; Receive

(defn receive-more? [socket]
  (pos? (ZMQ/getSocketOption socket ZMQ/ZMQ_RCVMORE)))

(defn blobs->map [blobs]
  (let [decoded-blobs             (map (fn [^Msg msg] (String. (.data msg))) blobs)
        [ids [delim sign & data]] (split-with (complement #{DELIM}) decoded-blobs)]
    (-> (zipmap [:header :parent-header :metadata :content]
                (map util/json->edn data))
        (assoc :signature sign :identities ids))))

(defn recv-message [socket]
  (loop [msg []]
    (when-let [blob (ZMQ/recv socket 0)]
      (let [msg (conj msg blob)]
        (if (receive-more? socket)
          (recur msg)
          (blobs->map msg))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Handler

(defn response-map [config {:keys [shell-socket iopub-socket]} shutdown-signal]
  (let [signer          (signer-fn (:key config))
        execution-count (volatile! 0)]
    {}))

(defn handler-fn [config socket-map shutdown-signal]
  (let [msg-type->response (response-map config socket-map shutdown-signal)]
    (fn [{{:keys [msg-type]} :header :as msg}]
      (log/info "Handling" msg)
      (if-let [handler (msg-type->response msg-type)]
        (handler msg)
        (log/info "No handler for" msg-type)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker

(defn start [config shutdown-signal]
  (future
    (log/info "Starting shell...")
    (try
      (with-open [^SocketSystem socket-system (create-sockets config)]
        (let [handler      (handler-fn config
                                       (dissoc socket-system :config :ctx)
                                       shutdown-signal)
              shell-socket (:shell-socket socket-system)]
          (log/debug "Entering loop...")
          (while (not (realized? shutdown-signal))
            (when-let [msg (recv-message shell-socket)]
              (handler msg)))))
      (catch Exception e (log/debug e))
      (finally
        (deliver shutdown-signal true)))))
