(ns clj-jupyter-player.shell
  (:require [clojure.pprint :as pprint]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [clj-jupyter-player.util :as util]
            [datascript.core :as d])
  (:import [java.util Date]
           [java.net ServerSocket InetSocketAddress]
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
  (init [{{:keys [stdin-port iopub-port hb-port control-port shell-port transport ip secret-key]} :config
           :as                                    this}]
    (let [signer          (signer-fn secret-key)
          ctx            (ZMQ/createContext)
          stdin-socket   (ZMQ/socket ctx ZMQ/ZMQ_DEALER)
          iopub-socket   (ZMQ/socket ctx ZMQ/ZMQ_SUB)
          hb-socket      (ZMQ/socket ctx ZMQ/ZMQ_REQ)
          control-socket (ZMQ/socket ctx ZMQ/ZMQ_DEALER)
          shell-socket   (ZMQ/socket ctx ZMQ/ZMQ_DEALER)
          addr         (partial str transport "://" ip ":")
          stdin-socket-connected   (.connect stdin-socket   (addr stdin-port))
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
          _ (when iopub-socket-connected
              (.setSocketOpt iopub-socket ZMQ/ZMQ_SUBSCRIBE (.getBytes "")))]
      (assoc this
             :signer signer
             :ctx ctx
             :stdin-socket   stdin-socket
             :iopub-socket   iopub-socket
             :hb-socket      hb-socket
             :control-socket control-socket
             :shell-socket   shell-socket)))
  (close [{:keys [ctx ports] :as this}]
    (doseq [socket (vals (dissoc this :signer :ctx :config :ports))]
      (ZMQ/close socket))
    (ZMQ/term ctx)
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

(defn recv-message [^SocketBase socket]
  (log/info "recv-message socket: " socket " checkTag: " (.checkTag socket))
  (loop [msg []]
    (log/info "recv-message-loop: " msg)
    (when-let [blob (try (ZMQ/recv socket ZMQ/ZMQ_DONTWAIT) (catch IllegalStateException ise (log/error "Tried to read from closed socket") nil))]
      (let [msg (conj msg blob)]
        (if (receive-more? socket)
          (recur msg)
          (blobs->map msg))))))

;; my attempt
(defn receive-from-socket
  [socket ch-req ch-close]
  (async/go-loop [blob (try (ZMQ/recv socket ZMQ/ZMQ_DONTWAIT) (catch IllegalStateException ise (log/error "Tried to read from closed socket") nil))
                  msg []
                  shutdown? (async/poll! ch-close)]
                 ;;(when (not (nil? blob))
                 ;;  (log/info "blob: " blob)
                 ;;  (log/info "msg: " msg)
                 ;;  (log/info "shutdown?: " shutdown?))

                 (if (nil? blob)
                   (recur (try (ZMQ/recv socket ZMQ/ZMQ_DONTWAIT) (catch IllegalStateException ise (log/error "Tried to read from closed socket") nil))
                          []
                          (async/poll! ch-close))
                   (if (receive-more? socket)
                     (recur (try (ZMQ/recv socket ZMQ/ZMQ_DONTWAIT) (catch IllegalStateException ise (log/error "Tried to read from closed socket") nil))
                            (conj msg blob)
                            (async/poll! ch-close))
                     (do
                       (async/>!! ch-req (blobs->map (conj msg blob)))
                          (recur (try (ZMQ/recv socket ZMQ/ZMQ_DONTWAIT) (catch IllegalStateException ise (log/error "Tried to read from closed socket") nil))
                          []
                          (async/poll! ch-close)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Handler

(defn response-map [config {:keys [shell-socket iopub-socket]} shutdown-signal]
  ;;(log/info "response map config: " config)
  (let [signer          (signer-fn (:secret-key config))]
    {"status" (fn [{{:keys [session msg-id]} :parent-header
                    {:keys [execution-state]}  :content}]
                (when (= (:session config) session)
                  (d/transact! (:conn config) [[:db/add -1 :jupyter.response/execution-state execution-state]
                                               [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]])))
     "execute_input" (fn [{{:keys [session msg-id]} :parent-header
                           {:keys [execution-count code]}  :content}])
     "execute_reply" (fn [{{:keys [session msg-id]}         :parent-header
                           {:keys [status execution-count]} :content
                           :as msg}]
                       ;;(log/info "execute_reply: " msg)
                       (when (= (:session config) session)
                         (d/transact! (:conn config) [[:db/add -1 :jupyter.response/status status]
                                                      [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]])))
     "stream" (fn [{{:keys [session msg-id]} :parent-header
                    content                  :content
                    :as msg}]
                ;;(log/info "stream: " msg)
                (when (= (:session config) session)
                  (d/transact! (:conn config) [[:db/add -1 :jupyter.response/stream content]
                                               [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]])))
     "shutdown_reply" (fn [{{:keys [session msg-id]} :parent-header
                            content                  :content
                            :as msg}]
                        ;;(log/info "shutdown_reply: " msg)
                        (when (= (:session config) session)
                          (d/transact! (:conn config) [[:db/add -1 :jupyter.response/status "ok"]
                                                       [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]])))}))

(defn handler-fn [config socket-map shutdown-signal]
  (let [msg-type->response (response-map config socket-map shutdown-signal)]
    (fn [{{:keys [msg-type]} :header :as msg}]
      ;;(log/info "Handling" msg)
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

(defn create-shutdown-request-msg
  [session restart?]
  (let [header (create-header session "shutdown_request")
        parent-header {}
        metadata {}
        content {"restart" restart?}]
    {:header header
     :parent-header parent-header
     :metadata metadata
     :content content}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Dispatch

(defmulti command :command)
(defmethod command :send [{:keys [shell-socket signer session source conn cell-eid]}]
  (let [;;_ (log/info "sending: " source)
        msg (create-execute-request-msg session (first source))
        ;;_ (log/info "msg: " msg)
        ]
    (d/transact! conn [[:db/add -1 :jupyter/msg-id (get-in msg [:header "msg_id"])]
                       [:db/add -1 :jupyter.player/sent (Date.)]
                       [:db/add cell-eid :notebook.cell.player/execute-request -1]])
    (send-message shell-socket msg signer)
    true))
(defmethod command :shutdown [{:keys [control-socket signer session conn]}]
  (let [msg (create-shutdown-request-msg session true)]
    (d/transact! conn [[:db/add -1 :jupyter/msg-id (get-in msg [:header "msg_id"])]
                       [:db/add -1 :jupyter.player/sent (Date.)]
                       [:db/add -1 :notebook.player/shutdown-request true]])
    (send-message control-socket msg signer)
    true))
(defmethod command :stop [_] false)
(defmethod command :default [_] (log/error "Shell failed to understand your command") true)
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
              iopub-socket (:iopub-socket socket-system)
              hb-socket (:hb-socket socket-system)
              control-socket (:control-socket socket-system)
              shell-socket (:shell-socket socket-system)
              _ (log/info "iopub-socket: " iopub-socket)
              ;;_ (log/info "shell-socket: " shell-socket)
              ;;_ (log/info "shell-socket check tag: " (.checkTag ^SocketBase shell-socket))
              ;;_ (log/info "iopub-socket check tag: " (.checkTag iopub-socket))
              ch-close (async/chan)
              ch-req (async/chan)
              ]

          (async/go-loop [request (async/<! ch-req)]
                         (handler request)
                         (recur (async/<! ch-req)))
          (log/debug "Entering loop...")
          (receive-from-socket iopub-socket ch-req ch-close)
          (receive-from-socket shell-socket ch-req ch-close)
          (receive-from-socket hb-socket ch-req ch-close)
          ;;(async/go-loop [shutdown (realized? shutdown-signal)]
          ;;  (if shutdown
          ;;    (log/info "shutdown response listener")
          ;;    (do
          ;;      (log/info "call recv-message")
          ;;      (when-let [msg (recv-message shell-socket)]
          ;;        (handler msg))
          ;;      (log/info "is blocking?")
          ;;      (when-let [msg (recv-message iopub-socket)]
          ;;        (handler msg))
          ;;      (recur (realized? shutdown-signal)))))
          (async/go-loop [request (async/<! (:ch config))]
            ;;(log/info "current notebook: " (d/pull @(:conn config) '[* {:notebook/cells [*]}] [:db/ident :notebook]))
            (if (command (assoc request :signer (:signer socket-system)
                                        :session (:session config)
                                        :shell-socket shell-socket
                                        :control-socket control-socket
                                        :conn (:conn config)))
              (recur (async/<! (:ch config)))
              (log/info "shutdown request listener"))))
        ;; hang until we get shutdown signal
        (while (not (realized? shutdown-signal)))
        )
      (catch Exception e (log/debug (util/stack-trace-to-string e)))
      (finally
        (deliver shutdown-signal true)))))
