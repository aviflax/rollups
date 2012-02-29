#!/usr/bin/env java -server -cp lib/* clojure.main

(comment "Copyright © Avi Flax and other contributors

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the 'Software'), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

See the file LICENSE in the root of this project for the full license.")


(ns rollup
    (:use [clojure.java.io :only [reader]])
	(:use [clojure.string :only [split-lines]])
    (:use [clj-time.core :only [interval minutes hours days weeks end plus start within? default-time-zone]])
    (:use [clj-time.format :only [formatter parse unparse]])
    (:import (org.joda.time DateTime Minutes Hours Days Weeks)))


(def ^:private apache-access-log-date-formatter (formatter "dd/MMM/yyyy:HH:mm:ss Z" (default-time-zone)))


(defn extract-date-from-apache-access-log-line [line]
    (let [regex-result (second (re-find #"\[([^\]]+)\]" line))]
        (try 
            (parse apache-access-log-date-formatter regex-result)
            (catch Exception e (str "No date found in " line)))))


(defn increment-window [{:keys [interval count]}]
    {:interval interval
     :count (inc count)})


(defn date-to-window-start [^DateTime date-time period]
    (let [base (-> date-time (.withMillisOfSecond 0) (.withSecondOfMinute 0))]
        (condp instance? period
            Minutes base
            Hours (.withMinuteOfHour base 0)
            Days (-> base (.withMinuteOfHour 0) (.withHourOfDay 0))
            Weeks (-> base (.withMinuteOfHour 0) (.withHourOfDay 0) (.withDayOfWeek 1))
            (throw (IllegalArgumentException. (str period " is not a supported period type"))))))


(defn make-window [date-time period]
    (let [start (date-to-window-start date-time period)
          end   (plus start period)]
        {:interval (interval start end)
         :count 1}))


(defn ^:private replace-last [coll value]
    (assoc coll (dec (count coll)) value))


(defn rollup-reduce [period results date-time]
    (if (instance? DateTime date-time)
        (update-in results [:windows]
            (fn [windows]
                (let [last-window (peek windows)]
                    (if (and (seq windows) (within? (:interval last-window) date-time))
                        (replace-last windows (increment-window last-window))
                        (conj windows (make-window date-time period))))))
        (update-in results [:errors] #(conj % (str date-time)))))


(defn rollup-dates [dates period]
    (reduce (partial rollup-reduce period) {:windows [] :errors []} dates))


(defn rollup-lines [lines period]
	(rollup-dates (map extract-date-from-apache-access-log-line lines) period))


(defn rollup-string
	"Rolls up a string containing one event timestamp per line"
	[string period]
	(rollup-lines (split-lines string) period))


(defn rollup-reader
	"Given a reader containing one event timestamp on each line,
    calculates the number of events within each time window of the specified length."
	[reader period]
	(let [lines (line-seq reader)]
    	(rollup-lines lines period)))


(defn print-rollup-as-csv 
    ([windows]
        (print-rollup-as-csv windows "\t"))
    ([windows separator]
        (println "Start" separator "End"  separator  "Count")
        (let [date-formatter (formatter "yyyy-MM-dd HH:mm")]
            (doseq [{:keys [interval count]} windows]
                (let [date-formatter (formatter "yyyy-MM-dd HH:mm")
                      start (unparse date-formatter (start interval))
                      end (unparse date-formatter (end interval))]
                    (println start separator end separator count))))))


(defn ^:private parse-window-spec [spec]
    (let [matches (re-find #"^(\d+)([mhdw])$" spec)]
        (if (= (count matches) 3)
            (let [num (Integer/parseInt (str (matches 1)))
                  unit (matches 2)]
                (case unit
                    "m" (Minutes/minutes num)
                    "h" (Hours/hours num)
                    "d" (Days/days num)
                    "w" (Weeks/weeks num)
                    (throw (IllegalArgumentException. (str unit " is not a valid window spec unit.")))))
            (throw (IllegalArgumentException. (str spec " is not a valid window spec."))))))


(defn ^:private println-err [string]
	(binding [*out* *err*]
		(println (str string))))


(defn ^:private println-err-exit [string]
    (println-err string)
    (System/exit 1))


(defn ^:private args-to-period [args]
    (if
        (and
			(= (count args) 2)
			(= (first args) "-w"))
        (try
            (parse-window-spec (second args))
            (catch IllegalArgumentException e
                (println-err-exit (.getMessage e))))
        (println-err-exit "the argument -w [spec] is required")))


;; Not sure what this should be called so it’s called when this file is
;; run as a script but not when it’s used as a library; I’ve seen conflicting documentation.
(defn -main [args]
    (let [results (rollup-reader (reader *in*) (args-to-period args))]
        (print-rollup-as-csv (:windows results))
        (println-err (apply str (interpose "\n" (:errors results))))))


(defn ^:private running-as-script
	"This is hacky and brittle but it seems to work. I’d love a better
	way to do this; see http://stackoverflow.com/q/9027265"
	[]
	(let
		[known-namespaces
			#{"clojure.set"
			    "user"
				"clojure.main" 
				"clj-time.format" 
				"clojure.core" 
				"rollup" 
				"clj-time.core" 
				"clojure.java.io" 
				"clojure.string" 
				"clojure.core.protocols"}]
		(= (count (all-ns)) (count known-namespaces))))


(when (running-as-script) (-main *command-line-args*))
