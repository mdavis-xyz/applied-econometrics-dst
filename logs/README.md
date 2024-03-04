# Log files

The formal requirements were to provide log files.
These are the `stdout` log files from running our code.

## Logs for R

To run a script and save the output to logs, on Linux do:

```
R --no-save < 05-regressions.R > logs/05.txt
```

Otherwise, try adding `sink(here::here("logs/log.txt"))` up the top of your `.R` script, and `sink()` at the bottom.
(Which is what we generally did.)

## Git

Logs here are not saved to git automatically.
Because we don't want git clashes every time you re-run scripts and change logs slightly.
To save logs to be submitted in our final zip, copy them to `../logs_committed`.
