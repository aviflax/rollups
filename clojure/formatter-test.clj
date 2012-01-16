#!/usr/bin/env java -cp lib/* clojure.main

(ns test
    (:use [clj-time.core :only [interval minutes hours days weeks end plus start within? default-time-zone]])
    (:use [clj-time.format :only [formatter parse unparse]])
    (:import (org.joda.time DateTime Minutes Hours Days Weeks DateTimeZone)))

(println (formatter "dd/MMM/yyyy:HH:mm:ss Z"))

(formatter "dd/MMM/yyyy:HH:mm:ss Z" default-time-zone)
