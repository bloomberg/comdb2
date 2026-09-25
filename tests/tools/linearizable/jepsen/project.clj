(defproject comdb2 "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.1.6"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [javax.xml.bind/jaxb-api "2.4.0-b180830.0359"]
                 [slingshot "0.12.2"]
                 [com.bloomberg.comdb2/cdb2jdbc "2.8.0"]]
  :jvm-opts ["--add-modules=java.sql"]
  :main comdb2.cli)
