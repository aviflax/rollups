#!/usr/bin/env java -cp lib/* clojure.main

(comment "
Copyright © Avi Flax and other contributors

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
    (:use [clj-time.core :only [interval days end plus start within?]])
    (:use [clj-time.format :only [formatter parse unparse]])
    (:import (org.joda.time Hours Days Weeks DateTime)))


(defrecord Results [windows errors])


(defrecord Window [interval count])


(defn extract-date [line]
    (let [date-formatter (formatter "dd/MMM/yyyy:HH:mm:ss Z")
          regex-result (second (re-find #"\[([^\]]+)\]" line))]
        (try 
            (parse date-formatter regex-result)
            (catch Exception e (str "No date found in " line)))))


(defn increment-window [window]
    (Window. (:interval window) (inc (:count window))))


(defn date-to-window-start [date-time period]
    (let [base (-> date-time (.withMillisOfSecond 0) (.withSecondOfMinute 0))]
        (condp instance? period
            Hours (.withMinuteOfHour base 0)
            Days (-> base (.withMinuteOfHour 0) (.withHourOfDay 0))
            Weeks (-> base (.withMinuteOfHour 0) (.withHourOfDay 0) (.withDayOfWeek 0))
            base
            )))


(defn make-window [date-time period]
    (let [start (date-to-window-start date-time period)
          end   (plus start period)]
            (Window. (interval start end) 1)))


(defn rollup-reduce [period results date-time]
    (let [windows (:windows results)
          errors  (:errors  results)]
        (if
            (instance? DateTime date-time)
            (Results.
                (if
                    (and (seq windows) (within? (:interval (last windows)) date-time))
                    (assoc windows (dec (count windows)) (increment-window (last windows)))
                    (assoc windows (count windows) (make-window date-time period)))
                (:errors results))
            (Results.
                windows
                (assoc errors (count errors) (str date-time))))))
            


(defn rollup-dates [dates period]
    (reduce (partial rollup-reduce period) (Results. [] []) dates))


(defn rollup-stream [stream period]
    (let [lines (line-seq (reader stream))]
        (rollup-dates (map extract-date lines) period)))


(defn rollup-to-csv [windows separator]
    ; TODO: make separator optional, with the default value "\t"
    (let [date-formatter (formatter "yyyy-MM-dd HH:mm")]
        (reduce
            #(str
                %
                (unparse date-formatter (start (:interval %2)))
                separator
                (unparse date-formatter (end (:interval %2)))
                separator
                (:count %2)
                "\n")
            (str "Start" separator "End"  separator  "Count" "\n")
            windows)))


(def results (rollup-stream *in* (days 1)))

(println (rollup-to-csv (:windows results) "\t"))

(binding [*out* *err*]
    (println (apply str (interpose "\n" (:errors results)))))