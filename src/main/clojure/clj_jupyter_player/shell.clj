(ns clj-jupyter-player.shell
  (:require [clojure.pprint :as pprint]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [clj-jupyter-player.util :as util])
  (:import [java.net ServerSocket InetSocketAddress]
           [java.util UUID]
           java.io.Closeable
           javax.crypto.Mac
           javax.crypto.spec.SecretKeySpec
           [zmq Msg SocketBase ZMQ Utils]))

(set! *warn-on-reflection* true)

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

(defn available-local-socket
  []
  (let [ss (ServerSocket.)
        _ (.setReuseAddress ss true)
        _ (.bind ss (InetSocketAddress. "127.0.0.1" 0))]
    ss))

(defn reserve-port
  "We obtain a free local port"
  [^ServerSocket tmp-socket]
  [(.getLocalPort tmp-socket) tmp-socket])

(defn reserve-ports
  "Returns a map of temporary sockets that are reserving a local port. It is the callers responsibility to close them."
  [n]
  (into {} (map reserve-port) (repeatedly n available-local-socket)))

(defn release-port
  [ports port]
  (.close ^java.io.Closeable (get ports port))
  port)

(defrecord SocketSystem [config]
  util/ILifecycle
  (init [{{:keys [ports port-order transport ip secret-key]} :config
           :as                                    this}]
    (let [signer          (signer-fn secret-key)
          ctx            (ZMQ/createContext)
          stdin-socket   (ZMQ/socket ctx ZMQ/ZMQ_DEALER)
          iopub-socket   (ZMQ/socket ctx ZMQ/ZMQ_PUB)
          hb-socket      (ZMQ/socket ctx ZMQ/ZMQ_REQ)
          control-socket (ZMQ/socket ctx ZMQ/ZMQ_DEALER)
          shell-socket   (ZMQ/socket ctx ZMQ/ZMQ_DEALER)
          addr         (partial str transport "://" ip ":")
          stdin-port   (get port-order 0)
          iopub-port   (get port-order 1)
          hb-port      (get port-order 2)
          control-port (get port-order 3)
          shell-port   (get port-order 4)
          ;;_ (ZMQ/setSocketOption shell-socket ZMQ/ZMQ_RCVTIMEO (int 250))
          stdin-socket-connected   (.connect stdin-socket   (addr stdin-port))
          _ (log/info "stdin-socket-connected returned")
          iopub-socket-connected   (.connect iopub-socket   (addr iopub-port))
          hb-socket-connected      (.connect hb-socket      (addr hb-port))
          control-socket-connected (.connect control-socket (addr control-port))
          shell-socket-connected   (.connect shell-socket   (addr shell-port))
          _ (when (every? identity [stdin-socket-connected
                                    iopub-socket-connected
                                    hb-socket-connected
                                    control-socket-connected
                                    shell-socket-connected])
              (log/info "all sockets connected"))
          _ (log/info [stdin-socket-connected
                                    iopub-socket-connected
                                    hb-socket-connected
                                    control-socket-connected
                                    shell-socket-connected])
          ]
      (assoc this
             :signer signer
             :ctx ctx
             :ports ports
             :stdin-socket   stdin-socket
             :iopub-socket   iopub-socket
             :hb-socket      hb-socket
             :control-socket control-socket
             :shell-socket   shell-socket)))
  (close [{:keys [ctx ports] :as this}]
    (doseq [socket (vals (dissoc this :signer :ctx :config :ports))]
      (ZMQ/close socket))
    (ZMQ/term ctx)
    (doseq [port (keys ports)]
      (release-port ports port))
    (log/info "All shell sockets closed.")))

(defn create-sockets [config]
  (util/init (->SocketSystem config)))

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
  ;;(log/info "response map config: " config)
  (let [signer          (signer-fn (:secret-key config))
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
;;; Create messsages

(defn create-header
  [session msg-type]
  {"msg_id" (str (UUID/randomUUID))
   "username" "clj-jupyter-player"
   "session" session
   "date" (util/now)
   "msg_type" msg-type
   "version" "5.0"})


(defn create-execute-request-msg
  [session code]
  (let [header (create-header session "execute_request")
        parent-header {}
        metadata {}
        content {"code" code
                 "silent" false
                 "store_history" true
                 "user_expressions" {}
                 "allow_stdin" false
                 "stop_on_error" true}]
    {:header header
     :parent-header parent-header
     :metadata metadata
     :content content}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Dispatch

(defmulti command :command)
(defmethod command :send [{:keys [shell-socket signer session source]}]
  (let [;;_ (log/info "sending: " source)
        msg (create-execute-request-msg session (first source))
        ;;_ (log/info "msg: " msg)
        ]
    (send-message shell-socket msg signer)
    true))
(defmethod command :stop [_] false)
(defmethod command :default [_] (log/error "Failed to understand your command") true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker

(defn start [config shutdown-signal]
  (log/info "config: " config)
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
            (async/go-loop [request (async/<! (:ch config))]
              (if (command (assoc request :signer (:signer socket-system)
                                          :session (:session config)
                                          :shell-socket shell-socket))
                (recur (async/<! (:ch config)))
                (log/info "shutdown request listener")))
            (when-let [msg (recv-message shell-socket)]
              (handler msg)))))
      (catch Exception e (log/debug (util/stack-trace-to-string e)))
      (finally
        (deliver shutdown-signal true)))))
