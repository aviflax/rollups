###
I want to analyze a bunch of Apache access logs.

Each line looks like this:
168.75.67.132 - - [23/Nov/2011:17:29:24 -0500] "POST /fnic-1/pxcentral/notifications/policy.updated HTTP/1.1" 200 69 "-" "ShortBus/1.1 (Noelios-Restlet-Engine/1.1.5;Java 1.6.0_20;Windows 2003 5.2)"

For now, I'll do my filtering outside this program — I just need this to calculate the time-window rollup.
###


LineStream = require('linestream')


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
        when 'm' then num * 1000 * 60
        when 'h' then num * 1000 * 60 * 60
        when 'd' then num * 1000 * 60 * 60 * 24
        when 'w' then num * 1000 * 60 * 60 * 24 * 7


###
There are 2 big problems with both of these functions right now:
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
###
rollupImperative = (dates, window="1h") ->
    windowMillis = windowToMillis window
    
    currentWindow =
        start: dates[0]
        end: new Date(dates[0].getTime() + windowMillis)
        count: 0
        
    windows = [currentWindow]
    
    # best not to assume that the dates are passed in sorted
    dates.sort (a, b) -> a - b
    
    ###
    TODO: there's a MAJOR bug here. Every time a date is encountered which is beyond the current window,
        it's assumed that it belongs to the immediately subsequent window. But that is not necessarily the case.
        And when it isn't, the function will create the next window with an erronous count.
        The function probably needs to be changed to loop through a set of windows, not through the set of dates.
    ###
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


# See comment above rollupImperative
rollupFunctional = (dates, window="1h") ->
    windowMillis = windowToMillis window
    
    makeWindow = (start) ->
        start: start
        end: new Date((start.getTime() + windowMillis) - 1)
        count: 0
    
    windows = (makeWindow new Date dateMillis for dateMillis in [dates[0].getTime()..dates[dates.length-1].getTime()] by windowMillis)

    ###
    this has the (small) advantage of not requiring the dates to be sorted
        but it's far less efficient when using small windows
        because we have to filter _all_ the dates for each window
    ###
    windows.map (window) ->
        start: window.start
        end: window.end
        count: (date for date in dates when date > window.start and date < window.end).length


linestream = new LineStream process.stdin
dates = []
linestream.on 'data', (line) -> dates.push extractDate line
linestream.on 'end', () -> console.log rollupFunctional dates, '1h'
process.stdin.resume()
