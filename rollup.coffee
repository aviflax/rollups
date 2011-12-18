#!/usr/bin/env coffee

###
rollup.coffee — given a line-delimited list of dates from Apache httpd access logs, count how many requests occurred in a given time window.

Each line looks like this:
168.75.67.132 - - [23/Nov/2011:17:29:24 -0500] "POST /fnic-1/pxcentral/notifications/policy.updated HTTP/1.1" 200 69 "-" "ShortBus/1.1 (Noelios-Restlet-Engine/1.1.5;Java 1.6.0_20;Windows 2003 5.2)"
        

POSSIBLE TO DOS:
* Support parallellization (off by default)
* Support other event timestamp formats
* Support other output formats (starting with JSON)
* Support cmd-line arg for csv separator
* Add an option to specify whether weeks should start on Sunday or Monday
* Support rollup windows of N months
* Support the input already being a rollup, of which we'd do a bigger rollup
** so you might store a per-minute rollup in a file, and generate a per-hour rollup from that
** kinda like re-reduce
** This'd work really well with a companion tool, something which would support incremental processing
   by keeping track of where you were in a file or stream, and grabbing only that

###


LineStream = require('linestream')
optimist = require('optimist')


extractDate = (line) ->
    dateRegex = /// \[
                    (\d{2}) #day
                    /
                    (\w{3}) #month
                    /
                    (\d{4}) #year
                    :
                    (\d{2}) #hour
                    :
                    (\d{2}) #minute
                    :
                    (\d{2}) #second
                    \s
                    ([-+]\d{4}) #time zone offset
                    \]
                ///
    
    matches = dateRegex.exec(line)
    
    return null if !matches
    
    day = matches[1]
    
    month =
        switch matches[2]
            when 'Jan' then 0
            when 'Feb' then 1
            when 'Mar' then 2
            when 'Apr' then 3
            when 'May' then 4
            when 'Jun' then 5
            when 'Jul' then 6
            when 'Aug' then 7
            when 'Sep' then 8
            when 'Oct' then 9
            when 'Nov' then 10
            when 'Dec' then 11
            
    year = matches[3]
    hour = matches[4]
    minute = matches[5]
    second = matches[6]
    offset = matches[7]
    
    # I had been passing in a string with the offset, which worked in general,
    # but there was a problem with days containing a daylight savings time switch.
    new Date(year, month, day, hour, minute, second)


extractWindowUnits = (windowSpec) -> windowSpec.charAt windowSpec.length - 1


windowToMillis = (window) ->
    num = parseInt window
    unit = extractWindowUnits window
    
    switch unit
        when 'm' then num * 1000 * 60
        when 'h' then num * 1000 * 60 * 60
        when 'd' then num * 1000 * 60 * 60 * 24
        when 'w' then num * 1000 * 60 * 60 * 24 * 7
        else throw new Error("#{window} is not a valid unit")



dateToWindowStart = (date, windowSpec) ->
    # need to create a new date because the setters mutate state
    start = new Date date.getTime()
    
    start.setUTCMilliseconds 0 
    start.setUTCSeconds 0 
    
    switch extractWindowUnits windowSpec
        when 'h'
            start.setUTCMinutes 0
        when 'd'
            start.setUTCMinutes 0
            start.setUTCHours 0
        when 'w'
            start.setUTCMinutes 0
            start.setUTCHours 0
            # change the day to the prior Sunday
            # JS appears to do the right thing: even if start was, for example, Thursday the first, then changing the date to Sunday also properly changes the month to the prior month.
            start.setUTCDate start.getUTCDate() - start.getUTCDay()
    
    #console.log "Converted date #{date.toUTCString()} to window start #{start.toUTCString()}"
    
    return start


makeWindow = (startDate, windowSpec, startCount=0) ->
    start = dateToWindowStart startDate, windowSpec 
    
    result = 
        start: start
        end: new Date((start.getTime() + windowToMillis windowSpec) - 1)
        count: startCount
        
    #console.log "\nCreated window ", result, " from date #{startDate.toUTCString()} with windowSpec #{windowSpec}"
    
    return result


copyWindow = (window) ->
    start: window.start
    end: window.end
    count: window.count



rollup = (windows, date, windowSpec='1d') ->
    # TODO: don't mutate windows, make a copy first
    
    matchingWindows = windows.filter (window) -> date >= window.start and date < window.end
    
    if matchingWindows.length
        window = matchingWindows[0]
    else
        #console.log "\n\n-- #{date.toUTCString()} --\nno matching window found in:\n", windows
        #if windows.length > 2 then throw new Error('ARGH')
        window = makeWindow date, windowSpec
        windows.push window
        
    window.count++
    
    # TODO: fill in any windows in between which might have been missed
    
    return windows

    
    


toCsv = (windows, separator='\t') ->
    'Start' + separator + 'End' + separator + 'Count\n' +
        windows.reduce ((previous, window) ->
            previous +
            formatDateTimeForCsv(window.start) +
            separator +
            formatDateTimeForCsv(window.end) +
            separator +
            window.count +
            '\n'), ''



padZeroLeft = (number) -> (if number < 10 then '0' else '') + number


formatDateTimeForCsv = (date) ->
    '' +
    date.getFullYear() + 
    '-' +
    padZeroLeft(date.getMonth() + 1) +
    '-' +
    padZeroLeft(date.getDate()) +
    ' ' +
    padZeroLeft(date.getHours()) +
    ':' +
    padZeroLeft(date.getMinutes())


### SCRIPT BODY ###

optimist.options 'w',
    alias: 'window',
    demand: true,
    describe: 'Time window size for rollups. E.g. 1m, 2h, 3d, 4w'

argv = optimist.argv

linestream = new LineStream process.stdin
windows = []

linestream.on 'data', (line) ->
    date = extractDate line
    if date then windows = rollup windows, date, argv.w
    #console.log "date: #{date}"
    #console.log "window: ", makeWindow(date, argv.w), "\n\n"

linestream.on 'end', () -> process.stdout.write toCsv windows
process.stdin.resume()
