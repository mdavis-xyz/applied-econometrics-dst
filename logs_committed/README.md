# Log files

The formal requirements were to provide log files.
These are the `stdout` log files from running our code.

`01a.txt.gz` is a gzip-compressed log file. And even that was only of a partial run. Because the full log file would be about 1GB.

Note that when you run the scripts, they save logs into `../logs`.
This folder contains log files copy-pasted from there, into here.
The reason being that we want our log files saved into git, so that we're all on the same page about what zip to submit.
But we don't want git clashes from slight modifications to the log files every time we re-run our scripts.

So `../logs` contains what you have most recently generated.
