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

import io.Source
import io.Source._
import scala.util.matching.Regex
import org.joda.time._
import org.joda.time.format._


class Window(val interval:Interval, val count:Int) {
    def incremented : Window = new Window(interval, count + 1)
}


def extractDate(line:String) : Option[DateTime] = {
    val dateRegex = new Regex("""\[(.*)\]""")
    val dateString = dateRegex.findAllIn(line).matchData.next().subgroups(0)
    val formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
    
    try
        Some(formatter.parseDateTime(dateString))
    catch {
        case e => None
    }
}


def makeWindow(dateTime:DateTime, windowPeriod:ReadablePeriod, count:Int) : Window = {
    // TODO: consider making this the constructor of Window
    val start = dateToWindowStart(dateTime, windowPeriod)
    val end = start.plus(windowPeriod)
    new Window(new Interval(start, end), count)
}


def dateToWindowStart(dateTime:DateTime, windowPeriod:ReadablePeriod) : DateTime = {
    val base = dateTime.withSecondOfMinute(0).withMillisOfSecond(0)
    
    windowPeriod match {
        case period:Hours => base.withMinuteOfHour(0)
        case period:Days => base.withMinuteOfHour(0).withHourOfDay(0)
        case period:Weeks => base.withMinuteOfHour(0).withHourOfDay(0).withDayOfWeek(1)
        case _  => base
    }
}


def parseWindowSpec(windowSpec:String) : Option[(Int, String)] = {
    val windowSpecRegex = new Regex("""^(\d+)([mhdw])""")
    val matches = windowSpecRegex.findAllIn(windowSpec.toLowerCase()).matchData.next()

    if (matches.subgroups.length == 2)
        Some((matches.subgroups(0).toInt, matches.subgroups(1)))
    else
        None
}


def windowSpecToPeriod(windowSpec:String) : Option[ReadablePeriod] = {
    parseWindowSpec(windowSpec) match {
        case Some((windowSpecNum, windowSpecUnit)) => {
            windowSpecUnit match {
                case "m" => Some(Minutes.minutes(windowSpecNum))
                case "h" => Some(Hours.hours(windowSpecNum))
                case "d" => Some(Days.days(windowSpecNum))
                case "w" => Some(Weeks.weeks(windowSpecNum))
                case  _  => None
            }
        }
        case None => None
    }
}


def rollup(windows:List[Window], date:DateTime, windowPeriod:ReadablePeriod) : List[Window] = {
    if (windows.exists(_.interval.contains(date)))
        windows map { window => if (window.interval.contains(date)) window.incremented else window }
    else
        windows :+ makeWindow(date, windowPeriod, 1)
}


// TODO: actually convert to CSV
def rollupToCsv(windows:List[Window]) : String = windows.map(window ⇒ window.interval.toString() + " : " + window.count).mkString("\n")


def sourceToRollup(source:Source, windowPeriod:ReadablePeriod) : (List[Window], List[String]) = {
    var windows = List[Window]()
    var errors = List[String]()
    
    source.getLines().foreach(line => {
        extractDate(line) match {
            case Some(date) => windows = rollup(windows, date, windowPeriod)
            case None => errors :+ "No date found in " + line
        }
    })

    (windows, errors)
}




/*** BEGIN SCRIPT BODY ***/

// TODO: change to a command-line arg
val windowSpecArg = "1d"



val windowPeriod = windowSpecToPeriod(windowSpecArg) match {
    case Some(period) => period
    case None => {
        // TODO: HACK
        System.exit(1)
        new Period()
    }
}

println("Processing input")

val (rollup, errors) = sourceToRollup(stdin, windowPeriod)

System.out.print(rollupToCsv(rollup))
System.err.print(errors.mkString)