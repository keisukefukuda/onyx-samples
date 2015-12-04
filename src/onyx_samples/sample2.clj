(ns onyx-samples.sample2
  (:require [clojure.core.async :refer [chan >! >!! <! <!! go close!]]
            [onyx.api]
            [onyx.plugin.core-async :refer [take-segments!]]
            [com.stuartsierra.component :as component]
            [clojure.java.jdbc :as sql]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]))

;;; Onyx test to use SQL plugin

(def onyx-id (java.util.UUID/randomUUID))

(def db-file "/tmp/onyx.sqlite")

(def batch-size 10)

(defn my-func [{:keys [number] :as segment}]
  {:greeting (str "Hello " number)})

(defonce system nil)

(defonce out-ch (chan 100))
(defonce in-ch (chan 100))

(def workflow
  [[:in :my-func]
   [:my-func :out]])

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/medium :core.async
    :onyx/type :input
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}
   
   {:onyx/name :my-func
    :onyx/type :function
    :onyx/fn :onyx-samples.sample2/my-func
    :onyx/batch-size batch-size}
    
   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/medium :core.async
    :onyx/type :output
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}
   ])

(def lifecycles
  [{:lifecycle/task :in  :lifecycle/calls :onyx-samples.sample2/in-calls}
   {:lifecycle/task :in  :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out :lifecycle/calls :onyx-samples.sample2/out-calls}
   {:lifecycle/task :out :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-ch})

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def n-peers (->> workflow (mapcat identity) set count))

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

(defn setup-db-spec []
  {:subprotocol "sqlite"
   :subname db-file})

(defn init-db [db-spec]
  (try 
    (sql/execute! db-spec
                  ["create table input (id INTEGER PRIMARY KEY, number INTEGER)"])
    (catch java.sql.SQLException e true))
  (doseq [i (range 100)]
    (sql/insert! db-spec :input {:number i})))

(defrecord OnyxDevEnv [n-peers db-spec]
  component/Lifecycle
  
  (start [component]
    (println "Starting Onyx development environment")
    ;; Setup a new Sqlite database file
    (let [db-spec (setup-db-spec)]
      (init-db db-spec)
      (let [onyx-id (java.util.UUID/randomUUID)
            env (onyx.api/start-env env-config)
            peer-group (onyx.api/start-peer-group peer-config)
            peers (onyx.api/start-peers n-peers peer-group)]
        (assoc component
               :env env
               :peer-group peer-group
               :peers peers
               :onyx-id onyx-id
               :db-spec db-spec))))

  (stop [component]
    (println "Stopping Onyx development environment")
    (doseq [v-peer (:peers component)]
      (onyx.api/shutdown-peer v-peer))
    (onyx.api/shutdown-peer-group (:peer-group component))
    (onyx.api/shutdown-env (:env component))
    (assoc component :env nil :peer-group nil :peers nil :db-spec nil)))

(defn submit-jobs []
  (go
    (loop []
      (let [db-spec (:db-spec system)
            rows (sql/query db-spec ["select * from input limit 10;"])]
        (println (count rows) "rows are read.")
        (if (empty? rows)
          (>! in-ch :done)
          (do
            (doseq [r rows]
              (try
                (sql/execute! db-spec ["delete from input where id = ?" (:id r)])
                (catch java.lang.Exception e
                  (println e)))
              (>! in-ch r))
            (recur))))))
  (let [job {:workflow workflow
             :catalog catalog
             :lifecycles lifecycles
             :task-scheduler :onyx.task-scheduler/balanced}]
    (println "Submitting")
    (onyx.api/submit-job peer-config job)
    (take-segments! out-ch)))

(defn init []
  (alter-var-root #'system (constantly (map->OnyxDevEnv {:n-peers n-peers}))))

(defn start []
  (when (nil? system)
    (init))
  (alter-var-root #'system (fn [s] (component/start s)))
  nil)

(defn stop []
  (alter-var-root #'system (fn [s] (component/stop s)))
  nil)
