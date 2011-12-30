#!/usr/bin/env scala -cp lib/* -deprecation
!#

/*
Copyright © Avi Flax and other contributors

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

See the file LICENSE in the root of this project for the full license.
*/


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
import org.joda.time._
import org.joda.time.format._


def extractDate(line:String):Option[DateTime] = {
    val dateRegex = new Regex("""\[(.*)\]""")
    val dateString = dateRegex.findAllIn(line).matchData.next().subgroups(0)
    val formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
    
    try
        Some(formatter.parseDateTime(dateString))
    catch {
        case e => None
    }
}


def makeWindow(dateTime:DateTime, windowSpec:String, startCount:Int = 0) : (Interval, Int) = {
    val start = dateToWindowStart(dateTime, windowSpec)

    val end:DateTime = null
    
    (new Interval(start, end), startCount)
}


def dateToWindowStart(dateTime:DateTime, windowSpec:String) : DateTime = null


def parseWindowSpec(windowSpec:String) : Option[(Int, String)] = {
    val windowSpecRegex = new Regex("""^(\d+)([mhdw])""")
    val matches = windowSpecRegex.findAllIn(windowSpec.toLowerCase()).matchData.next()

    if (matches.subgroups.length == 2)
        Some((matches.subgroups(0).toInt, matches.subgroups(1)))
    else
        None
}



def windowSpecToPeriod(windowSpec:String) : Option[Period] = {
    parseWindowSpec(windowSpec) match {
        case Some(windowSpec) => {
            val (windowSpecNum, windowSpecUnit) = windowSpec
            windowSpecUnit match {
                case "m" => Some(Period.minutes(windowSpecNum))
                case "h" => Some(Period.hours(windowSpecNum))
                case "d" => Some(Period.days(windowSpecNum))
                case "w" => Some(Period.weeks(windowSpecNum))
                case  _  => None
            }
        }
        case None => None
    }
}



def rollup(windows:List[(Interval, Int)], date:DateTime, windowSpec:String = "1d") : List[(Interval, Int)] = {
    if (windows.exists(_._1.contains(date)))
        windows.map(window => if (window._1.contains(date)) (window._1, window._2 + 1) else window)
    else
        windows :+ makeWindow(date, windowSpec, 1)
}


def toCsv(windows:List[(Interval, Int)]):String = null // TODO: return the CSV



/*** BEGIN SCRIPT BODY ***/

var windows = List[(Interval, Int)]()
var errors = List[String]()

println("Processing input")

stdin.getLines().foreach(line => {
    extractDate(line) match {
        case Some(date) => windows = rollup(windows, date)
        case None => errors :+ "No date found in " + line
    }
})

print(toCsv(windows))