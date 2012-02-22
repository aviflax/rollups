#!/usr/bin/env coffee

###
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
###


LineStream = require 'linestream'
optimist = require 'optimist'


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
    
    matches = dateRegex.exec line
    
    return null if !matches
    
    day = matches[1]
    month = matches[2]
    year = matches[3]
    hour = matches[4]
    minute = matches[5]
    second = matches[6]
    offset = matches[7]
    
    # important to preserve the offsets so we're basically working with local times
    new Date("#{month} #{day}, #{year} #{hour}:#{minute}:#{second}#{offset}")


extractWindowUnits = (windowSpec) -> windowSpec.charAt windowSpec.length - 1

extractWindowNum = (windowSpec) -> parseInt windowSpec

windowToMillis = (windowSpec) ->
    num = extractWindowNum windowSpec
    unit = extractWindowUnits windowSpec
    
    switch unit
        when 'm' then num * 1000 * 60
        when 'h' then num * 1000 * 60 * 60
        when 'd' then num * 1000 * 60 * 60 * 24
        when 'w' then num * 1000 * 60 * 60 * 24 * 7
        else throw new Error "#{unit} is not a valid unit"


dateToWindowStart = (date, windowSpec) ->
    # need to create a new date because the setters mutate state
    start = new Date date
    
    start.setMilliseconds 0 
    start.setSeconds 0 
    
    switch extractWindowUnits windowSpec
        when 'h'
            start.setMinutes 0
        when 'd'
            start.setMinutes 0
            start.setHours 0
        when 'w'
            start.setMinutes 0
            start.setHours 0
            # change the day to the prior Sunday
            # JS appears to do the right thing: even if start was, for example, Thursday the first, then changing the date to Sunday also properly changes the month to the prior month.
            start.setDate start.getDate() - start.getDay()
        
    return start


makeWindow = (startDate, windowSpec, startCount=0) ->
    start = dateToWindowStart startDate, windowSpec 
    
    start: start
    end: addWindowDurationToStartDate start, windowSpec
    count: startCount


dateToLastMinuteOfThatDay = (date) ->
    # dumb hack to get around Daylight Savings Time issues
    endOfDay = new Date date
    endOfDay.setHours 23
    endOfDay.setMinutes 59
    endOfDay.setSeconds 59
    endOfDay.setMilliseconds 999
    endOfDay


addWindowDurationToStartDate = (startDate, windowSpec) ->
    num = extractWindowNum windowSpec
    
    switch extractWindowUnits windowSpec
        when 'm', 'h'
            new Date (startDate.getTime() + windowToMillis windowSpec) - 1
        when 'd'
            end = dateToLastMinuteOfThatDay startDate
            end.setDate end.getDate() + num - 1
            end
        when 'w'
            # if start is Sunday the 1st at 00:00:00 then end needs to be Saturday the 7th at 23:59:59
            # JS/V8 appear to do the right thing: even if start was, for example, Thursday the first, then changing the date to Sunday also properly changes the month to the prior month.
            # first get a date which is the last minute of the start date. This is an attempt to avoid problems with daylight savings time
            end = dateToLastMinuteOfThatDay startDate
            end.setDate end.getDate() + ((num * 7) - 1)
            end


incrementWindow = (window) ->
    start: window.start
    end: window.end
    count: window.count + 1



Array.prototype.updated = (index, value) ->
	# Return a copy of an array with an element at a specified index replaced with a new value
	this.slice(0, index).concat(value, this.slice(index + 1))


Array.prototype.replaceLast = (value) ->
    this.updated this.length - 1, value


rollup = (windows, date, windowSpec='1d') ->
    return windows if not date

    # we only check the last window for a match, so the data must be pre-sorted
    lastWindow = windows[windows.length - 1]

    if lastWindow and lastWindow.start <= date < lastWindow.end
        windows.replaceLast incrementWindow(lastWindow)
    else
        windows.concat makeWindow date, windowSpec, 1


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





###### SCRIPT BODY ######

optimist.options 'w',
    alias: 'window',
    demand: true,
    describe: 'Time window size for rollups. E.g. 1m, 2h, 3d, 4w'

argv = optimist.argv

linestream = new LineStream process.stdin

# TODO: this gets mutated (replaced) with every 'data' event below. Is that OK?
### It'd be more functional to treat the lines as a sequence. Gotta look into some
    way to do that in Node ###
windows = []

linestream.on 'data', (line) ->
    windows = rollup windows, extractDate(line), argv.w

linestream.on 'end', () -> process.stdout.write toCsv windows

process.stdin.resume()
