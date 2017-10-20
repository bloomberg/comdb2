(defproject comdb2 "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.6"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [slingshot "0.12.2"]
                 [com.bloomberg.comdb2/cdb2jdbc "2.0.0"]]
  :test-selectors {:a6 :a6-test
                   :g2 :g2-test
                   :register :register-test
                   :register-nemesis :register-test-nemesis
                   :dirty-reads :dirty-reads-test
                   :sets :sets-test
                   :sets-nemesis :sets-test-nemesis
                   :bank :test-bank
                   :bank-nemesis :test-bank-nemesis
                  }
  )
