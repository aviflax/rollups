#!/usr/bin/env scala
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

val exampleLine = """168.75.67.132 - - [23/Nov/2011:17:29:24 -0500] "POST /fnic-1/pxcentral/notifications/policy.updated HTTP/1.1" 200 69 "-" "ShortBus/1.1 (Noelios-Restlet-Engine/1.1.5;Java 1.6.0_20;Windows 2003 5.2)""""

// val input = stdin.getLines
val input = exampleLine.lines

//println(parse exampleLine)

input map parse rollupImperative

def parse(line:String):(Date, Int) = {
    val dateRegex = new Regex("""\[(.*)\]""")
    val dateString = dateRegex.findAllIn(line).matchData.next().subgroups(0)
    val dateFormat = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
    val date = dateFormat.parse(dateString)
    
    val statusCodeRegex = new Regex("""" ([0-9]{3}) """) //"
    val statusCodeString = statusCodeRegex.findAllIn(line).matchData.next().subgroups(0)
    val statusCode = statusCodeString.toInt
    
    (date, statusCode)
}

def rollupImperative(lines:List[(Date,Int)]):Iterable[(Date,Date,Int)] = {
    var result = List()
    
    var currentStart = lines(0)._1
    var currentCount = 0
    
    lines.foreach { line => {
        val (date,status) = line

        currentCount += 1

        if (dateDiff(current._1, date) >= 5.minutes) {
            result += (currentStart,date,currentCount)
            currentStart = date
            currentCount = 0
        }
    }}
    
    result
}






