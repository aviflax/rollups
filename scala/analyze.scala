#!/usr/bin/env scala -cp lib/* -deprecation
!#

/*

I want to analyze a bunch of Apache access logs.

Each line looks like this:
168.75.67.132 - - [23/Nov/2011:17:29:24 -0500] "POST /fnic-1/pxcentral/notifications/policy.updated HTTP/1.1" 200 69 "-" "ShortBus/1.1 (Noelios-Restlet-Engine/1.1.5;Java 1.6.0_20;Windows 2003 5.2)"

That's the "combined" format.

The rollup should be dynamic.

I want to build a few stats for each given time period (the rollup):
    - total requests
    - 50x errors
    - error rate (percentage of total which were errors)
    - 
    
Let's walk through this.
    for each line I want to:
        determine if it's in the current rollup slice or if I need to start a new slice
        increment the number of total requests
        if it's an error, increment the total number of errors
    

*/

import io.Source._
import scala.util.matching.Regex
import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time.Interval
import org.joda.time.DateTime


def extractDate(line:String):DateTime = {
    val dateRegex = new Regex("""\[(.*)\]""")
    val dateString = dateRegex.findAllIn(line).matchData.next().subgroups(0)
    val dateFormat = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
    
    dateFormat.parse(dateString)
    
    null // TODO: return a Joda DateTime
}


def makeWindow(date:DateTime, windowSpec:String, startCount:Int = 0) : (Interval, Int) = null


def rollup(windows:List[(Interval, Int)], date:DateTime, windowSpec:String = "1d") : List[(Interval, Int)] = {
    if (windows.exists(_._1.contains(date)))
        windows.map(window => if (window._1.contains(date)) (window._1, window._2 + 1) else window)
    else
        windows :+ makeWindow(date, windowSpec, 1)
}


def toCsv(windows:List[(Interval, Int)]):String = null // TODO: return the CSV


var windows = List[(Interval, Int)]()

println("Processing input")

stdin.getLines().foreach(line => windows = rollup(windows, extractDate(line)))

print(toCsv(windows))