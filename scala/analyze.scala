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
import org.joda.time._


def extractDate(line:String):Option[DateTime] = {
    val dateRegex = new Regex("""\[(.*)\]""")
    val dateString = dateRegex.findAllIn(line).matchData.next().subgroups(0)
    val dateFormat = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
    
    dateFormat.parse(dateString)
    
    None // TODO: return a Joda DateTime
}


def makeWindow(dateTime:DateTime, windowSpec:String, startCount:Int = 0) : (Interval, Int) = {
    val start = dateToWindowStart(dateTime, windowSpec)

    val end:DateTime = null
    
    (new Interval(start, end), startCount)
}


def dateToWindowStart(dateTime:DateTime, windowSpec:String) : DateTime = null


def windowSpecToPeriod(windowSpec:String) : Option[Object] = {
    // TODO: Consider using a Joda PeriodFormatter to parse this

    val windowSpecRegex = new Regex("""^(\d+)([mhdw])""")
    val matches = windowSpecRegex.findAllIn(windowSpec.toLowerCase()).matchData.next()

    if (matches.subgroups.length != 2) return None

    val windowSpecNum = matches.subgroups(0).toInt
    val windowSpecUnit = matches.subgroups(1)
    
    windowSpecUnit match {
        case "m" => Some(Minutes.minutes(windowSpecNum))
        case "h" => Some(Period.hours(windowSpecNum))
        case "d" => Some(Period.days(windowSpecNum))
        case "w" => Some(Period.weeks(windowSpecNum))
        case  _  => None
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