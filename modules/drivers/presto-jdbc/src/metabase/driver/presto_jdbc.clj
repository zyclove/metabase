(ns metabase.driver.presto-jdbc
    "Presto JDBC driver. See https://prestodb.io/docs/current/ for complete dox."
    (:require [clojure.java.jdbc :as jdbc]
              [clojure.set :as set]
              [clojure.string :as str]
              [clojure.tools.logging :as log]
              [metabase.db.spec :as db.spec]
              [metabase.driver :as driver]
              [metabase.driver.presto-common :as presto-common]
              [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
              [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
              [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
              [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
              [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
              [metabase.driver.sql-jdbc.sync.describe-database :as sql-jdbc.describe-database]
              [metabase.util.i18n :refer [trs]])
    (:import [java.sql Connection ResultSet]))

(driver/register! :presto-jdbc, :parent #{:presto-common :sql-jdbc ::legacy/use-legacy-classes-for-read-and-set})

;;; Presto API helpers

(defmethod sql-jdbc.sync/database-type->base-type :presto-jdbc [_ field-type]
  ;; seems like we can just delegate to the Presto implementation for this?
  (presto-common/presto-type->base-type (if (keyword? field-type) (name field-type) field-type)))

;;; Kerberos related definitions
(def ^:private ^:const kerb-props->url-param-names
  {:kerberos-principal "KerberosPrincipal"
   :kerberos-remote-service-name "KerberosRemoteServiceName"
   :kerberos-use-canonical-hostname "KerberosUseCanonicalHostname"
   :kerberos-credential-cache-path "KerberosCredentialCachePath"
   :kerberos-keytab-path "KerberosKeytabPath"
   :kerberos-service-principal-pattern "KerberosServicePrincipalPattern"
   :kerberos-config-path "KerberosConfigPath"})

(defn- details->kerberos-url-params [details]
  (let [remove-blank-vals (fn [m] (into {} (remove (fn [[_ v]]
                                                       (or (nil? v) (= "" v))) m)))
        ks                (keys kerb-props->url-param-names)]
    (-> (select-keys details ks)
      remove-blank-vals
      (set/rename-keys kerb-props->url-param-names))))

#_(s/defn ^:private details->uri
          [{:keys [ssl host port kerberos] :as details} :- PrestoConnectionDetails, path]
          (let [conn-str  (str (if ssl "https" "http") "://" host ":" port path)
                addl-opts (cond
                           (Boolean. kerberos)
                           (let [ks (keys kerb-props->url-param-names)]
                                (-> (select-keys details ks)
                                    remove-blank-kerb-props
                                    (set/rename-keys kerb-props->url-param-names))))]
               (if-not (empty? addl-opts)
                       (let [opts-str (sql-jdbc.common/additional-opts->string :url addl-opts)]
                            (sql-jdbc.common/conn-str-with-additional-opts conn-str :url opts-str))
                       conn-str)))

(defn- prepare-addl-opts [{:keys [SSL kerberos additional-options] :as details}]
  (if kerberos
    (if-not SSL
      (throw (ex-info (trs "SSL must be enabled to use Kerberos authentication")
                      {:db-details details}))
      (let [det (update details
                        :additional-options
                        str
                        ;; add separator if there are already additional-options
                        (if-not (str/blank? additional-options) "&")
                        ;; convert Kerberos options map to URL string
                        (sql-jdbc.common/additional-opts->string :url (details->kerberos-url-params details)))]
        ;; finally, remove the standalone Kerberos properties from details map
        (apply dissoc (cons det (keys kerb-props->url-param-names)))))
    details))

(defn- jdbc-spec
  [{:keys [host port catalog]
    :or   {host "localhost", port 5432, catalog ""}
    :as   details}]
  (-> details
      (merge {:classname   "io.prestosql.jdbc.PrestoDriver"
              :subprotocol "presto"
              :subname     (db.spec/make-subname host port catalog)})
      prepare-addl-opts
      (dissoc :host :port :db :catalog :tunnel-enabled :engine :kerberos)
      sql-jdbc.common/handle-additional-options))

(defmethod sql-jdbc.conn/connection-details->spec :presto-jdbc
  [_ {ssl? :ssl, :as details-map}]
  (let [props (-> details-map
                  (update :port (fn [port]
                                    (if (string? port)
                                      (Integer/parseInt port)
                                      port)))
                  (assoc :SSL ssl?)
                  (dissoc :ssl))]
       (jdbc-spec props)))

(defn- have-select-privilege?
  "Checks whether the connected user has permission to select from the given `table-name`, in the given `schema`.
  Adapted from the legacy Presto driver implementation."
  [driver conn schema table-name]
  (try
   (let [sql (sql-jdbc.describe-database/simple-select-probe-query driver schema table-name)]
        ;; if the query completes without throwing an Exception, we can SELECT from this table
        (jdbc/reducible-query {:connection conn} sql)
        true)
   (catch Throwable _
     false)))

(defn- describe-schema
  "Gets a set of maps for all tables in the given `catalog` and `schema`. Adapted from the legacy Presto driver
  implementation."
  [driver conn catalog schema]
  (let [sql (presto-common/describe-schema-sql driver catalog schema)]
    (into #{} (comp (filter (fn [{table-name :table}]
                                (and (not (contains? presto-common/excluded-schemas schema))
                                     (have-select-privilege? driver conn schema table-name))))
                    (map (fn [{table-name :table}]
                             {:name        table-name
                              :schema      schema})))
              (jdbc/reducible-query {:connection conn} sql))))

(defn- all-schemas
  "Gets a set of maps for all tables in all schemas in the given `catalog`. Adapted from the legacy Presto driver
  implementation."
  [driver conn catalog]
  (let [sql (presto-common/describe-catalog-sql driver catalog)]
    (into #{}
          (map (fn [{:keys [schema] :as full}]
                   {:tables (describe-schema driver conn catalog schema)}))
          (jdbc/reducible-query {:connection conn} sql))))

(defmethod driver/describe-database :presto-jdbc
  [driver {{:keys [catalog schema] :as details} :details :as database}]
  (with-open [conn (-> (sql-jdbc.conn/db->pooled-connection-spec database)
                       jdbc/get-connection)]
    (let [schemas (remove presto-common/excluded-schemas (all-schemas driver conn catalog))]
      (reduce set/union schemas))))

;; Result set holdability must be HOLD_CURSORS_OVER_COMMIT
;; defining this method to omit the holdability param
(defmethod sql-jdbc.execute/prepared-statement :presto-jdbc
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn
                                sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
       (try
         (try
           (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
           (catch Throwable e
             (log/debug e (trs "Error setting prepared statement fetch direction to FETCH_FORWARD"))))
         (sql-jdbc.execute/set-parameters! driver stmt params)
         stmt
         (catch Throwable e
           (.close stmt)
           (throw e)))))

;; and similarly for statement
(defmethod sql-jdbc.execute/statement :presto-jdbc
  [_ ^Connection conn]
  (let [stmt (.createStatement conn
                               ResultSet/TYPE_FORWARD_ONLY
                               ResultSet/CONCUR_READ_ONLY)]
       (try
         (try
           (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
           (catch Throwable e
             (log/debug e (trs "Error setting statement fetch direction to FETCH_FORWARD"))))
         stmt
         (catch Throwable e
           (.close stmt)
           (throw e)))))

(defmethod driver/can-connect? :sql-jdbc
  [driver details]
  (sql-jdbc.conn/can-connect? driver details))

(prefer-method driver/supports? [:presto-common :set-timezone] [:sql-jdbc :set-timezone])
