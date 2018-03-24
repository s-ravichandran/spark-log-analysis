# Web Server Log Analysis

Here, I analyze the Apache server log of NASA's website over a period of time in 1995. The log file can be found [here](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html). Some of the tasks that I have performed are 
	
	- Find the number of successful requests handled (and subsequently, the success ratio)
	- Find the most accessed entity on the web server (from the paths)
	- Calculate the total volume of data provided by the server

The tasks were split across 4 cores (and the RDD into 4 partitions). I've used regular expressions (provided in CS105x) for parsing the log file.
