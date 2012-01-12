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

import io.Source
import io.Source._
import java.io.InputStream
import scala.util.matching.Regex
import org.joda.time._
import org.joda.time.format._


class Window(val interval:Interval, val count:Int) {
    def this(dateTime:DateTime, windowPeriod:ReadablePeriod, count:Int) = this(new Interval(dateToWindowStart(dateTime, windowPeriod), dateToWindowStart(dateTime, windowPeriod).plus(windowPeriod)), count)
    def incremented : Window = new Window(interval, count + 1)
}


class RollupResults(val windows:List[Window] = List[Window](), val errors:List[String] = List[String]())


abstract class LineParseResult
case class EventDateTime(val dateTime:DateTime) extends LineParseResult
case class ErrorMessage(val error:String) extends LineParseResult


def rollup(dateTimes:List[DateTime], windowPeriod:ReadablePeriod) : RollupResults = dateTimes.map(new EventDateTime(_)).foldLeft(new RollupResults())(rollup(windowPeriod))


/** The main rollup fold function.
  * Intended to be used by a fold, “curried” with the windowPeriod arg.
  * So: foldLeft(List[Window]).rollup(Minutes.minutes(5))
  */
def rollup(windowPeriod:ReadablePeriod)(results:RollupResults, lineParseResult:LineParseResult) : RollupResults = {
    lineParseResult match {
        case EventDateTime(dateTime) ⇒ {
            // TODO: requires that the input be sorted. Would be more flexible to search from the end (but slower).
            if (results.windows.length > 0 && results.windows.last.interval.contains(dateTime))
                new RollupResults(results.windows.updated(results.windows.length - 1, results.windows.last.incremented), results.errors)
            else
                new RollupResults(results.windows :+ new Window(dateTime, windowPeriod, 1), results.errors)            
        }
        case ErrorMessage(error) ⇒ new RollupResults(results.windows, results.errors :+ error)
    }
}


def rollup(stream:InputStream, windowPeriod:ReadablePeriod) : RollupResults = rollup(Source.fromInputStream(stream), windowPeriod)


def rollup(source:Source, windowPeriod:ReadablePeriod) : RollupResults =
    source.getLines().map(
           (line:String) ⇒ extractDate(line) match {
                case Some(dateTime) ⇒ new EventDateTime(dateTime)
                case None ⇒ new ErrorMessage("No date found in " + line)
            }
        ).foldLeft(new RollupResults())(rollup(windowPeriod))


def extractDate(line:String) : Option[DateTime] = {
    val dateRegex = new Regex("""\[([^\]]+)\]""")
    val matches = dateRegex.findAllIn(line)
    if (!matches.hasNext) return None
    val dateString = matches.matchData.next().subgroups(0)
    val formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z")
    
    try
        Some(formatter.parseDateTime(dateString))
    catch {
        case e ⇒ None
    }
}


def dateToWindowStart(dateTime:DateTime, windowPeriod:ReadablePeriod) : DateTime = {
    val base = dateTime.withSecondOfMinute(0).withMillisOfSecond(0)
    
    windowPeriod match {
        case period:Hours ⇒ base.withMinuteOfHour(0)
        case period:Days ⇒ base.withMinuteOfHour(0).withHourOfDay(0)
        case period:Weeks ⇒ base.withMinuteOfHour(0).withHourOfDay(0).withDayOfWeek(1)
        case _ ⇒ base
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
        case Some((windowSpecNum, windowSpecUnit)) ⇒ {
            windowSpecUnit match {
                case "m" ⇒ Some(Minutes.minutes(windowSpecNum))
                case "h" ⇒ Some(Hours.hours(windowSpecNum))
                case "d" ⇒ Some(Days.days(windowSpecNum))
                case "w" ⇒ Some(Weeks.weeks(windowSpecNum))
                case  _  ⇒ None
            }
        }
        case None => None
    }
}


def rollupToCsv(windows:List[Window], separator:String = "\t") : String = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
    
    windows.foldLeft("Start" + separator + "End" + separator + "Count\n") {(string, window) ⇒
        string +
        formatter.print(window.interval.getStart()) +
        separator +
        formatter.print(window.interval.getEnd()) +
        separator +
        window.count +
        "\n"
    }
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

val results = rollup(stdin, windowPeriod)

System.out.print(rollupToCsv(results.windows))
System.err.print(results.errors.mkString("\n"))
