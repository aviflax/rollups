#!/usr/bin/env coffee

###
rollup.coffee — given a line-delimited list of dates from Apache httpd access logs, count how many requests occurred in a given time window.

Each line looks like this:
168.75.67.132 - - [23/Nov/2011:17:29:24 -0500] "POST /fnic-1/pxcentral/notifications/policy.updated HTTP/1.1" 200 69 "-" "ShortBus/1.1 (Noelios-Restlet-Engine/1.1.5;Java 1.6.0_20;Windows 2003 5.2)"


Bugs:
    * memory & performance are poor because it doesn't stream
    * the rollup windows start wherever the first event is, going on from there in regular intervals
        * so if the first event is at 19:03 and we're doing a 1-day rollup, each 24-hour window starts at 19:03
        * instead, I need to start each window by finding the first event, figure out from that when the first window should start
        * UPDATE: this is fixed EXCEPT for 'w'
        
There are 2 big problems with this right now:
    1) it requires loading the entire set of dates into memory all at once before processing
    2) for every window we scan through the entire set of dates
This uses a ton of memory and CPU time. If we need to process a large data set into a set of small windows,
    it could take a lot of resources and time.

A possibly better approach might be to build the set of windows on the fly, as we go, and process the dates
    one at a time.

So maybe the function would take a seq of rollup data, and a new date, and figure out where it fits.
    It would search for a window in which it fit — if it found one, voila, done.
    If not, it'd create it and add it to the seq.

Essentially, instead of looping through all the dates for each window, loop through all the windows for each date.
    This'd almost certainly save memory, since it could "stream", although it might not be more CPU efficient.


TO DO:
* Support parallellization (off by default)
* Support other event timestamp formats
* Support other output formats (starting with JSON)
* Support cmd-line arg for csv separator
    
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
    month = matches[2]
    year = matches[3]
    hour = matches[4]
    minute = matches[5]
    second = matches[6]
    offset = matches[7]
    
    new Date("#{day} #{month} #{year} #{hour}:#{minute}:#{second}#{offset}")



extractWindowUnits = (window) -> window.charAt window.length - 1


windowToMillis = (window) ->
    num = parseInt window
    unit = extractWindowUnits window
    
    switch unit
        when 'm' then num * 1000 * 60
        when 'h' then num * 1000 * 60 * 60
        when 'd' then num * 1000 * 60 * 60 * 24
        when 'w' then num * 1000 * 60 * 60 * 24 * 7
        else throw new Error("#{window} is not a valid unit")



dateToWindowStart = (date, window) ->
    # need to create a new date because the setters mutate state
    start = new Date date
    
    start.setMilliseconds 0 
    start.setSeconds 0 
    
    switch extractWindowUnits window
        when 'h'
            start.setMinutes 0
        when 'd', 'w'
            start.setMinutes 0
            start.setHours 0
            # TODO: for 'w', should probably move to the beginning of the week, but should it be Sunday or Monday?
    
    return start



rollup = (dates, window='1d') ->
    if dates.length is 0 then return []
    
    windowMillis = windowToMillis window
    
    # this could be inline below but it's much more readable this way
    makeWindow = (start) ->
        start: start
        end: new Date((start.getTime() + windowMillis) - 1)
        count: 0
        
    firstWindowStart = dateToWindowStart(dates[0], window)
    
    # create all the windows with a 0 count
    windows = (makeWindow new Date dateMillis for dateMillis in [firstWindowStart.getTime()..dates[dates.length-1].getTime()] by windowMillis)
    
    ###
    this has the (small) advantage of not requiring the dates to be sorted
        but it's far less efficient when using small windows
        because we have to filter _all_ the dates for each window
    ###
    windows.map (window) ->
        start: window.start
        end: window.end
        count: (date for date in dates when date >= window.start and date < window.end).length



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


formatDateTimeForCsv = (date) -> "#{date.getFullYear()}-#{padZeroLeft(date.getMonth()+1)}-#{padZeroLeft(date.getDate())} #{padZeroLeft(date.getHours())}:#{padZeroLeft(date.getMinutes())}"


### SCRIPT BODY ###

optimist.options 'w',
    alias: 'window',
    demand: true,
    describe: 'Time window size for rollups. E.g. 1m, 2h, 3d, 4w'

argv = optimist.argv

linestream = new LineStream process.stdin
dates = []

linestream.on 'data', (line) ->
    date = extractDate line
    if date then dates.push date

linestream.on 'end', () -> process.stdout.write toCsv rollup dates, argv.w
process.stdin.resume()
