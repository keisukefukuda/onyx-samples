(ns onyx-samples.sample1-core-async
  (:require [clojure.core.async :refer [chan >! >!! <! <!! go close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [com.stuartsierra.component :as component]
            [clojure.pprint :as pp]
            [onyx.api]))

;; A very simple Onyx test code that takes segments from a channel
;; and just move them to another channel

(def in-ch (chan 500))
(def out-ch (chan 500))

(def system nil)

(def workflow
  [[:in :inc]
   [:inc :out]])

(def n-peers (->> workflow (mapcat identity) set count))

(def batch-size 10)
(def batch-timeout 50)

(def catalog
  [{:onyx/name :in
    :onyx/type :input
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-timeout batch-timeout
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/type :function
    :onyx/fn :onyx-samples.sample1-core-async/my-inc
    :onyx/batch-size batch-size}
    
   {:onyx/name :out
    :onyx/type :output
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-timeout batch-timeout
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}
   ])

(defn inject-in-ch [event lifecycle]
  (println "inject-in-ch is called.")
  {:core.async/chan in-ch})

(defn inject-out-ch [event lifecycle]
  (println "inject-out-ch is called.")
  {:core.async/chan out-ch})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx-samples.sample1-core-async/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx-samples.sample1-core-async/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   ])

(def onyx-id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id onyx-id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx/id onyx-id})

(defrecord OnyxDevEnv [n-peers]
  component/Lifecycle

  (start [component]
    (println "Starting Onyx development environment")
    (let [onyx-id (java.util.UUID/randomUUID)
          env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          peers (onyx.api/start-peers n-peers peer-group)]
      (assoc component :env env :peer-group peer-group
             :peers peers :onyx-id onyx-id)))

  (stop [component]
    (println "Stopping Onyx development environment")
    (doseq [v-peer (:peers component)]
      (onyx.api/shutdown-peer v-peer))
    (onyx.api/shutdown-peer-group (:peer-group component))
    (onyx.api/shutdown-env (:env component))
    (assoc component :env nil :peer-group nil :peers nil)))

(defn my-inc [segment]
  (update-in segment [:n] inc))

(defn init []
  (alter-var-root #'system (constantly (map->OnyxDevEnv {:n-peers n-peers}))))

(defn start []
  (when (nil? system)
    (init))
  (alter-var-root #'system (fn [s] (component/start s)))
  nil)

(defn stop []
  (alter-var-root #'system (fn [s] (when s (component/stop s))))
  nil)

(defn submit-jobs []
  (dotimes [i 20]
    (let [segment {:n i :greeting (str "Hello" i)}]
      (>!! in-ch segment)))
  (>!! in-ch :done)
  (let [job {:workflow workflow
             :catalog catalog
             :lifecycles lifecycles
             :task-scheduler :onyx.task-scheduler/balanced}]
    (println "Submitting")
    (onyx.api/submit-job peer-config job)))

(defn -main [& args]
  (init)
  (start)
  (submit-jobs)
  (pp/pprint (take-segments! out-ch))
  (stop)
  (shutdown-agents))

