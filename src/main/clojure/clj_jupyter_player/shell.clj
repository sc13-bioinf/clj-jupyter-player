(ns clj-jupyter-player.shell
  (:require [clojure.pprint :as pprint]
            [clojure.data.json :as json]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]
            [clj-jupyter-player.util :as util]
            [datascript.core :as d]
            [taoensso.timbre :as log])
  (:import [java.util Date]
           [java.net ServerSocket InetSocketAddress]
           [java.util UUID]
           java.io.Closeable
           javax.crypto.Mac
           javax.crypto.spec.SecretKeySpec
           [zmq Msg SocketBase Utils]
           [org.zeromq ZMQ ZContext]))

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
  "We obtain a free local port"
  []
  (let [ss (ServerSocket.)
        _ (.setReuseAddress ss true)
        _ (.bind ss (InetSocketAddress. "127.0.0.1" 0))]
    ss))

(defn reserve-port
  "Get the port bound by the socket"
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
          ^ZContext ctx            (ZContext.)
          stdin-socket   (.createSocket ctx ZMQ/DEALER)
          iopub-socket   (.createSocket ctx ZMQ/SUB)
          hb-socket      (.createSocket ctx ZMQ/REQ)
          control-socket (.createSocket ctx ZMQ/DEALER)
          shell-socket   (.createSocket ctx ZMQ/DEALER)
          _ (.setLinger iopub-socket (int 0))
          _ (.setLinger hb-socket (int 0))
          _ (.setLinger control-socket (int 0))
          _ (.setLinger shell-socket (int 0))
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
              (.subscribe iopub-socket ""))]
      (assoc this
             :signer signer
             :ctx ctx
             :stdin-socket   stdin-socket
             :iopub-socket   iopub-socket
             :hb-socket      hb-socket
             :control-socket control-socket
             :shell-socket   shell-socket)))
  (close [{:keys [^java.io.Closeable ctx] :as this}]
    (.close ctx)
    (log/info "Terminated shell socket context.")))

(defn create-sockets [config]
  (util/init (->SocketSystem config)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Messaging

(def DELIM "<IDS|MSG>")

;;; Send

(defn map->blobs [{:keys [identities] :as message} signer]
  (let [data-blobs (map (comp json/write-str util/edn->json message)
                        [:header :parent-header :metadata :content])
        signature  (signer data-blobs)]
    (concat identities [DELIM signature] data-blobs)))

(defn send-message [^org.zeromq.ZMQ$Socket socket message signer]
  (loop [[^String msg & r :as l] (map->blobs message signer)]
    (log/debug "Sending " msg)
    (if (seq r)
      (do (.sendMore socket msg)
          (recur r))
      (.send socket msg))))

(defn send-message-raw [^org.zeromq.ZMQ$Socket socket ^String msg]
  (log/debug "Sending raw msg " msg)
  (.send socket msg))

;;; Receive

(defn blobs->map [blobs]
  (let [decoded-blobs (map #(String. ^bytes % "UTF-8") blobs)]
    (if (and (= 1 (count decoded-blobs))
             (= "ping" (first decoded-blobs)))
      {:header {:msg-type "heartbeat"}}
      (let [[ids [delim sign & data]] (split-with (complement #{DELIM}) decoded-blobs)]
        (-> (zipmap [:header :parent-header :metadata :content]
                    (map (comp util/json->edn json/read-str) data))
          (assoc :signature sign :identities ids))))))

(defn register-socket-with-poller
  [^org.zeromq.ZMQ$Poller poller accumulator [poller-index ^org.zeromq.ZMQ$Socket socket]]
  (let [_ (.register poller socket (bit-or org.zeromq.ZMQ$Poller/POLLIN org.zeromq.ZMQ$Poller/POLLERR))]
    (assoc accumulator poller-index socket)))

(defn receive-more?
  [^org.zeromq.ZMQ$Socket socket]
  (.hasReceiveMore socket))

(defn receive-from-socket
  [^org.zeromq.ZMQ$Socket socket]
  (try
    (.recv socket ZMQ/DONTWAIT)
    (catch IllegalStateException ise
      (log/error "Tried to read from closed socket") nil)))

(defn receive-from-sockets
  [^org.zeromq.ZMQ$Poller poller socket-map ch-req]
  (log/debug "pre call to poll")
  (let [poll-f (future (.poll poller 200))
        poll-r (deref poll-f 10000 :timeout)]
    (when (= poll-r :timeout)
      (log/error "epoll bug?")
      (future-cancel poll-f)))
  (log/debug "post call to poll")
  (doseq [poller-index [0 1 2]]
    (let [^org.zeromq.ZMQ$Socket socket (get socket-map poller-index)]
      (when (.pollerr poller poller-index)
        (log/error "Got pollerr from: " poller-index))
      (when (.pollin poller poller-index)
        (loop [blob (receive-from-socket socket)
               msg []]
          (when-not (nil? blob)
            (if (receive-more? socket)
              (recur (receive-from-socket socket)
                     (conj msg blob))
              (when-not (async/offer! ch-req (blobs->map (conj msg blob)))
                (throw (Exception. "Failed to offer request to ch-req"))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Handler

(defn response-map [config {:keys [shell-socket iopub-socket]}]
  ;;(log/info "response map config: " config)
  (let [signer          (signer-fn (:secret-key config))]
    {"heartbeat" (fn [_] (log/debug "heartbeat"))
     "status" (fn [{{:keys [session msg-id]} :parent-header
                    {:keys [execution-state]} :content}]
                (when (= (:session config) session)
                  (d/transact! (:conn config) [[:db/add -1 :jupyter.response/execution-state execution-state]
                                               [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]])))
     "display_data" (fn [{{:keys [session msg-id]} :parent-header
                          {:keys [data metadata]} :content}]
                      (when (= (:session config) session)
                        (d/transact! (:conn config)
                                     (conj (vec (util/tx-data-from-map -1 {:jupyter.response/data data
                                                                           :jupyter.response/metadata metadata}))
                                             [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]))))

     "error" (fn [{{:keys [session msg-id]} :parent-header
                   {:keys [status ename evalue traceback]} :content}]
               (when (= (:session config) session)
                 (d/transact! (:conn config)
                              (conj (vec (util/tx-data-from-map -1 {:jupyter.response/ename ename
                                                                    :jupyter.response/evalue evalue
                                                                    :jupyter.response/traceback traceback
                                                                    :jupyter.response/status status}))
                                    [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]))))
     "execute_input" (fn [{{:keys [session msg-id]} :parent-header
                           {:keys [execution-count code]}  :content}])
     "execute_reply" (fn [{{:keys [session msg-id]}         :parent-header
                           {:keys [status execution-count]} :content
                           :as msg}]
                       ;;(log/info "execute_reply: " msg)
                       (when (= (:session config) session)
                         (d/transact! (:conn config)
                                      (conj (vec (util/tx-data-from-map -1 {:jupyter.response/execution-count execution-count
                                                                            :jupyter.response/status status}))
                                            [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]))))
     "execute_result" (fn [{{:keys [session msg-id]}         :parent-header
                            {:keys [data metadata execution-count]} :content}]
                        (when (= (:session config) session)
                          (d/transact! (:conn config)
                                       (conj (vec (util/tx-data-from-map -1 {:jupyter.response/execution-count execution-count
                                                                             :jupyter.response/data data
                                                                             :jupyter.response/metadata metadata}))
                                             [:db/add [:jupyter/msg-id msg-id] :jupyter/response -1]))))
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

(defn handler-fn [config socket-map]
  (let [msg-type->response (response-map config socket-map)]
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
        msg (create-execute-request-msg session (clojure.string/join "" source))
        ;;_ (log/info "msg: " msg)
        ]
    (d/transact! conn [[:db/add -1 :jupyter/msg-id (get-in msg [:header "msg_id"])]
                       [:db/add -1 :jupyter.player/sent (Date.)]
                       [:db/add cell-eid :notebook.cell.player/execute-request -1]])
    (send-message shell-socket msg signer)
    true))
(defmethod command :shutdown [{:keys [control-socket signer session conn loop-counter]}]
  (let [msg (create-shutdown-request-msg session false)]
    (d/transact! conn [[:db/add -1 :jupyter/msg-id (get-in msg [:header "msg_id"])]
                       [:db/add -1 :jupyter.player/sent (Date.)]
                       [:db/add -1 :notebook.player/shutdown-request true]])
    (reset! loop-counter 0)
    (send-message control-socket msg signer)
    true))
(defmethod command :stop [_] false)
(defmethod command :default [_] (log/error "Shell failed to understand your command") true)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Worker

(defn start [config]
  (let [_ (log/info "Starting shell...")
        socket-system (create-sockets config)
        handler (handler-fn config (dissoc socket-system :config :ctx))
        iopub-socket (:iopub-socket socket-system)
        hb-socket (:hb-socket socket-system)
        control-socket (:control-socket socket-system)
        shell-socket (:shell-socket socket-system)
        ch-req (async/chan 3)
        ^ZContext context (:ctx socket-system)
        ^org.zeromq.ZMQ$Poller poller (.createPoller context 3)
        sockets [iopub-socket shell-socket hb-socket]
        socket-map (reduce (partial register-socket-with-poller poller) {} (map-indexed vector sockets))
        loop-counter (atom 1)]
    (async/go-loop [request (async/<! ch-req)]
      (if (nil? request)
        (log/debug "shutdown request listener")
        (do
          (handler request)
          (recur (async/<! ch-req)))))
    (Thread/sleep 3000)
    (log/info "Entering shell loop...")
    (while (not (.. Thread currentThread isInterrupted))
      (receive-from-sockets poller socket-map ch-req)
      (when (and (-> loop-counter deref zero? not)
                 (zero? (mod (swap! loop-counter inc) 5)))
        (send-message-raw hb-socket "ping"))
      (if-let [v (async/poll! (:ch config))]
        (when-not (command (assoc v :signer (:signer socket-system)
                                    :session (:session config)
                                    :shell-socket shell-socket
                                    :control-socket control-socket
                                    :conn (:conn config)
                                    :loop-counter loop-counter))
          (log/info "shutdown shell-request listener")
          (.close context)
          (async/close! ch-req)
          (.. Thread currentThread interrupt))
        (Thread/sleep 1000)))
    (try
      (Thread/sleep 1000)
      (catch InterruptedException ie
        (log/debug "Cleared while loop interupt")))
    (log/info "Exiting shell loop")
    (Thread/sleep 1000)
    (let [response-status (d/q '[:find [(pull ?e [:jupyter.response/status]) ...]
                                 :where [?e :jupyter.response/status _]]
                               (-> config :conn d/db))]
      (if (every? (comp #(= % "ok") :jupyter.response/status) response-status)
        0
        1))))
