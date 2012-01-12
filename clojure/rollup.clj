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
    (use [clojure.java.io :only [reader]])
    (use clj-time.core)
    (use clj-time.format)
    (:import (org.joda.time Hours Days Weeks)))


(defrecord Window [interval count])


(defn extract-date [line]
    (let [date-formatter (formatter "dd/MMM/yyyy:HH:mm:ss Z")]
        (parse date-formatter
            (second (re-find #"\[([^\]]+)\]" line)))))


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


(defn rollup-dates [period windows date-time]
    (if
        (and (seq windows) (within? (:interval (last windows)) date-time))
        (assoc windows (dec (count windows)) (increment-window (last windows)))
        (assoc windows (count windows) (make-window date-time period))))


(defn rollup-stream [stream period]
    (let [lines (line-seq (reader stream))]
        (reduce (partial rollup-dates period) [] (map extract-date lines))))


(println (rollup-stream *in* (days 1)))
