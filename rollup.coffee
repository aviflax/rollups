###

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
    

###

LineStream = require('linestream')

exampleLine = '''168.75.67.132 - - [23/Nov/2011:17:29:24 -0500] "POST /fnic-1/pxcentral/notifications/policy.updated HTTP/1.1" 200 69 "-" "ShortBus/1.1 (Noelios-Restlet-Engine/1.1.5;Java 1.6.0_20;Windows 2003 5.2)"'''

# val input = stdin.getLines
#input = exampleLine.lines

#println(parse exampleLine)

#input map parse rollupImperative

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


windowToMillis = (window) ->
    num = parseInt window
    unit = window.charAt window.length - 1
    
    switch unit
        when 'm'
            num * 60 * 1000
        when 'h'
            num * 60 * 60 * 1000
        when 'd'
            num * 24 * 60 * 60 * 1000
        when 'w'
            num * 7 * 24 * 60 * 60 * 1000



rollupImperative = (dates, window="1h") ->
    windowMillis = windowToMillis window
    
    currentWindow =
        start: dates[0]
        end: new Date(dates[0].getTime() + windowMillis)
        count: 0
        
    windows = [currentWindow]
    
    dates.forEach (date) ->
        if date < currentWindow.end
            currentWindow.count++
        else
            newWindow = 
                start: new Date(currentWindow.end.getTime() + 1)
                end: new Date(currentWindow.end.getTime() + 1 + windowMillis)
                count: 1
                
            windows.push newWindow
            currentWindow = newWindow
    
    return windows


rollupMathy = (dates) ->
    #dates.map (date) -> [date, Math.round date.getTime()/1.00001]

    dates.map (date) -> [date, (date.getTime() + '').substr(0, 14), new Date(parseInt((date.getTime() + '').substr(0, 14)))]


linestream = new LineStream process.stdin

dates = []

linestream.on 'data', (line) -> dates.push extractDate line

linestream.on 'end', () -> console.log rollupImperative dates, '1d'

process.stdin.resume()