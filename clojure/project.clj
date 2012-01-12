(defproject com.aviflax/rollups "0.1.0"
  :description "A tool and a library for doing time-window rollups of events. So if you have a list of timestamps, you can analyze it to determine how many occurred within time windows of a specified length."
  :license {:name "MIT License"
            :url "http://www.opensource.org/licenses/mit-license.php"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [clj-time "0.3.4"]])