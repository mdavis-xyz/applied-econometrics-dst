# Log files

The formal requirements were to provide log files.
These are the `stdout` log files from running our code.

`01a.txt.gz` is a gzip-compressed log file. And even that was only of a partial run. Because the full log file would be about 1GB.

## Logs for R

To run a script and save the output to logs, on Linux do:

```
R --no-save < 05-regressions.R > logs/05.txt
```

Otherwise, try adding `sink(here::here("logs/log.txt"))` up the top of your `.R` script, and `sink()` at the bottom.

