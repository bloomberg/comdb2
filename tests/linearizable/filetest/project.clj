(defproject jepsen.filetest "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :main jepsen.filetest
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.4"]
                 [knossos "0.2.9-SNAPSHOT" :exclusions [org.slf4j/slf4j-log4j12]]])
