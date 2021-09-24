(ns clojars.search
  (:refer-clojure :exclude [index])
  (:require [clojars
             [config :refer [config]]
             [stats :as stats]]
            [clojars.db :as db]
            [clojure
             [set :as set]
             [string :as string]]
            [msync.lucene :as lucene]
            [msync.lucene.document :as document]
            [msync.lucene.analyzers :as analyzers]
            [com.stuartsierra.component :as component]
            [msync.lucene.indexer :as indexer]))

(defprotocol Search
  (index! [t pom])
  (search [t query page])
  (delete!
    [t group-id]
    [t group-id artifact-id]))

(def content-fields [:artifact-id :group-id :version :description :url #(->> % :authors (string/join " "))])

(def doc-opts
  {:stored-fields  content-fields
   ;;:suggest-fields [:artifact-id]
   ;;:context-fn     :group-id
   })

(def field-settings {:artifact-id {:analyzed false}
                     :group-id {:analyzed false}
                     :version {:analyzed false}
                     :at {:analyzed false}})

(defonce default-analyzer (analyzers/standard-analyzer))
(defonce keyword-analyzer (analyzers/keyword-analyzer))
(defonce analyzer
  (analyzers/per-field-analyzer default-analyzer
                                {}
                                #_ (into {}
                                         (for [[field {:keys [analyzed]}] field-settings
                                               :when (false? analyzed)]
                                           [field keyword-analyzer]))))

(def renames {:name       :artifact-id
              :jar_name   :artifact-id
              :group      :group-id
              :group_name :group-id
              :created    :at
              :homepage   :url})

(defn delete-from-index [index group-id & [artifact-id]]
  #_(binding [lucene/*analyzer* analyzer]
    (lucene/search-and-delete index
                              (cond-> (str "group-id:" group-id)
                                     artifact-id (str " AND artifact-id:" artifact-id)))))

(defn update-if-exists
  [m k f & args]
  (if (contains? m k)
    (apply update m k f args)
    m))

(defn jar->doc [jar]
  (-> jar
      (set/rename-keys renames)
      (update :licenses #(mapv :name %))
      (update-if-exists :at (memfn getTime))
      (dissoc :dependencies :scm)))

(defn index-jar [index jar]
  (prn "INDEX-JAR" jar)
  (let [doc (jar->doc jar)]
    (lucene/index! index [doc] doc-opts)))

#_(defn- track-index-status
  [{:keys [indexed last-time] :as status}]
  (let [status' (update status :indexed inc)]
    (if (= 0 (rem indexed 1000))
        (let [next-time (System/currentTimeMillis)]
          (printf "Indexed %s jars (%f/second)\n" indexed (float (/ (* 1000 1000) (- next-time last-time))))
          (flush)
          (assoc status' :last-time next-time))
        status')))

;; TODO: why do we have index construction in the system if we don't use it here?
(defn generate-index [db]
  (let [index-path ((config) :index-path)
        _ (printf "index-path: %s\n" index-path)
        index (indexer/create! {:type     :disk
                                :path     index-path
                                :analyzer analyzer})
        jar-data (map jar->doc (db/all-jars db))]
    (lucene/index! index jar-data doc-opts)

    #_(with-open [index (lucene/disk-index index-path)]
      ;; searching with an empty index creates an exception
      (lucene/add index {:dummy true})
      (let [{:keys [indexed start-time]}
            (reduce
              (fn [status jar]
                (try
                  (jar->doc index jar)
                  (catch Exception e
                    (printf "Failed to index %s/%s:%s - %s\n" (:group_name jar) (:jar_name jar) (:version jar)
                            (.getMessage e))
                    (.printStackTrace e)))
                (track-index-status status))
              {:indexed 0
               :last-time (System/currentTimeMillis)
               :start-time (System/currentTimeMillis)}
              (db/all-jars db))
            seconds (float (/ (- (System/currentTimeMillis) start-time) 1000))]
        (printf "Indexing complete. Indexed %s jars in %f seconds (%f/second)\n"
                indexed seconds (/ indexed seconds))
        (flush))
      (lucene/search-and-delete index "dummy:true"))))


;; We multiply this by the fraction of total downloads an item gets to
;; compute its download score. It's an arbitrary value chosen to give
;; subjectively good search results.
;;
;; The most downloaded item has about 3% of the total downloads, so
;; the maximum score is about 50 * 0.03 = 1.5.

(def download-score-weight 50)

(defn download-values [stats]
  #_(let [total (stats/total-downloads stats)]
    (ValueSourceQuery.
     (proxy [FieldCacheSource] ["download-count"]
       (getCachedFieldValues [cache _ reader]
         (let [ids (map vector
                        (.getStrings cache reader "group-id")
                        (.getStrings cache reader "artifact-id"))
               download-score (fn [i]
                                (let [score
                                      (inc
                                         (* download-score-weight
                                            (/ (apply
                                                (comp inc stats/download-count)
                                                stats
                                                (nth ids i))
                                               (max 1 total))))]
                                  score))]
           (proxy [DocValues] []
             (floatVal [i]
               (download-score i))
             (intVal [i]
               (download-score i))
             (toString [i]
               (str "download-count="
                    (download-score i))))))))))

(defn date-in-epoch-ms
  [iso-8601-date-string]
  (-> (java.time.ZonedDateTime/parse iso-8601-date-string)
      .toInstant
      .toEpochMilli
      str))

(defn lucene-time-syntax
  [start-time end-time]
  (format "at:[%s TO %s]"
          (date-in-epoch-ms start-time)
          (date-in-epoch-ms end-time)))

(defn replace-time-range
  "Replaces human readable time range in query with epoch milliseconds"
  [query]
  (let [matches (re-find #"at:\[(.*) TO (.*)\]" query)]
    (if (or (nil? matches) (not= (count matches) 3))
      query
      (try (->> (lucene-time-syntax (nth matches 1) (nth matches 2))
                (string/replace query (nth matches 0)))
           (catch Exception _ query)))))

; http://stackoverflow.com/questions/963781/how-to-achieve-pagination-in-lucene
(defn -search [stats index query page]
  (if (string/blank? query)
    []
    (doto (lucene/search index
                         {:artifact-id query}
                         { ;;:page             page
                          :results-per-page 24
                          :hit->doc         document/document->map})
      (prn "RESULT")))
  #_(if (empty? query)
    []
    (binding [lucene/*analyzer* analyzer]
      (with-open [searcher (IndexSearcher. index)]
        (let [per-page 24
              offset (* per-page (- page 1))
              parser (QueryParser. lucene/*version*
                                   "_content"
                                   lucene/*analyzer*)
              query  (.parse parser (replace-time-range query))
              query  (CustomScoreQuery. query (download-values stats))
              hits   (.search searcher query (* per-page page))
              highlighter (#'lucene/make-highlighter query searcher nil)]
          (doall
           (let [dhits (take per-page (drop offset (.scoreDocs hits)))]
             (with-meta (for [hit dhits]
                          (#'lucene/document->map
                           (.doc searcher (.doc hit))
                           (.score hit)
                           highlighter))
               {:total-hits (.totalHits hits)
                :max-score (.getMaxScore hits)
                :results-per-page per-page
                :offset offset}))))))))

(defrecord LuceneSearch [stats index-factory index]
  Search
  (index! [t pom]
    (index-jar index pom))
  (search [t query page]
    (-search stats index query page))
  (delete! [t group-id]
    (delete-from-index index group-id))
  (delete! [t group-id artifact-id]
    (delete-from-index index group-id artifact-id))
  component/Lifecycle
  (start [t]
    (if index
      t
      (assoc t :index (index-factory))))
  (stop [t]
    (when index
      ;;(.close index)
      )
    (assoc t :index nil)))

(defn lucene-component []
  (map->LuceneSearch {}))
