# Rollups

I needed a tool to calculate time-window event rollups, and I wanted to learn functional programming with various languages.

Now aspiring to provide libs that other people could actually use!

## Example

If you have a list of event timestamps, say something like this:

    127.0.0.1 - - [01/Dec/2011:00:00:11 -0500] "GET / HTTP/1.0" 304 
    127.0.0.1 - - [01/Dec/2011:00:00:24 -0500] "GET /feed/rss2/ HTTP/1.0" 301 
    127.0.0.1 - - [01/Dec/2011:00:00:25 -0500] "GET /feed/ HTTP/1.0" 304 
    127.0.0.1 - - [01/Dec/2011:00:00:30 -0500] "GET /robots.txt HTTP/1.0" 200 
    127.0.0.1 - - [01/Dec/2011:00:00:30 -0500] "GET / HTTP/1.0" 200 
    127.0.0.1 - - [01/Dec/2011:00:01:35 -0500] "POST /wp-cron.php?doing_wp_cron HTTP/1.0" 200 

then you could run one of these tools like so:

    gzcat event-data | rollup.coffee -w 1d
    
and you’d get something like this:

    Start	End	Count
    2011-12-01 00:00	2011-12-01 23:59	2823
    2011-12-02 00:00	2011-12-02 23:59	2572
    2011-12-03 00:00	2011-12-03 23:59	2621
    2011-12-04 00:00	2011-12-04 23:59	3027

    
## Aspirations
I plan to:

* support various input and output formats (but not too many)
* support usage as either a command-line tool or as a programmatic library
* make it fast


## Language Variants

A big part of my goal was to learn functional programming by implementing this functionality in various languages.

The goal is for each variant to be functionally equivalent, but I’m not quite there yet.

So here's some notes on the status of each one:


### CoffeeScript / Node
* Fast and efficient — about 1 second using a single core with my simple test
* Supports passing the window spec via the command line — something I haven’t added to the other variants yet


### Clojure
* Second-fastest so far — about 3.3 seconds with my simple test
* Window spec is currently hard-coded to 1 day
* Should already be usable as a library


### Scala
* Slow — about 130 seconds with my simple test
* Window spec is currently hard-coded to 1 day


### Python
* Not yet functional. Not sure I’ll finish this one as writing functional Python seems like it might be kinda annoying.


## To Dos

### Definite
* Support other event timestamp formats
* Support other output formats (starting with JSON)
* Support cmd-line arg for csv separator
* Refactor to be usable as a general-purpose library
  * CoffeeScript: either in Node or in a browser


### Possible
* Decide whether data must be passed in sorted or not (would allow for some optimizations)
* Add behaviour tests!!
* Support parallellization (off by default)
* Add an option to specify whether weeks should start on Sunday or Monday
* Support rollup windows of N months
* Support the input already being a rollup, of which we'd do a bigger rollup
  * so you might store a per-minute rollup in a file, and generate a per-hour rollup from that
  * kinda like re-reduce
  * This'd work really well with a companion tool, something which would support incremental processing by keeping track of where you were in a file or stream, and grabbing only the new part of the data, then updating the pointer
