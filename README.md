# Superman

Supervisor for Gearman workers.

Acts kinda like the gearman cli in worker mode but supports multiple functions,
different programs (per function), and a concurrency setting per function, plus
loading and reloading the function definitions from MySQL.

## Install

Clone this and install deps with pnpm.

## Configure

Copy [`superman.sample.toml`](./superman.sample.toml) to `superman.toml` in the
CWD or to `/etc/superman.toml`, read it, and change it accordingly.

The mysql table should have the following columns:

 - `id` (opaque to superman, so int, uint, uuid, whatever)
 - either of:
   + `ns` and `method`, both strings (text, varchar, etc), not null, OR
   + `name`, string (text, varchar, etc)
 - `active` (boolean)
 - `concurrency` (int)
 - `executor` (string)

Concurrency is per superman instance, there's no mechanism to coordinate with
other instances (yet?).

Executor must be the absolute path to an executable. If the file it points to
doesn't exist or isn't executable, the function definition is silently skipped,
which provides a loose way to run some functions on some hosts only based on
capability (ish).

If `ns` and `method` are provided, they are joined by `::`. If `name` is
provided, it's used as-is.

## Run

Run with `node .` in the cloned exec or however you usually like to run NodeJS
things.

Kill it with USR2 to force a reload.

It will gracefully shut down on SIGINT and SIGTERM.

## Executor interface

Executor executables are run with three arguments:

 - the name of the gearman function
 - the unique id of the job
 - the length in bytes of the workload to expect on STDIN

If the workload length is omitted or zero, there's no need to read it.

The program should output logging or crash info on stderr, that will be
collected and provided via gearman exception in case of hard crash. Otherwise
if the job itself fails that should be reported on stdout as per below.

Stdout is reserved for gearman interaction. Write JSON objects compactly, with
no internal newlines, separated by newlines, in these formats:

 - `{"type":"complete",...}` for completion. The rest of the object minus the
   `type` key will be JSON encoded and sent as `WORK_COMPLETE`. The convention
   is to have either `"data":...` for success or `"error":...` for failure.
 - `{"type":"update","data":...}` for mid-run updates. The value of the `data`
   key will be JSON encoded and sent as `WORK_DATA`.
 - `{"type":"progress","numerator":123,"denominator":456}` for mid-run quotient
   progress updates. The values will be sent alongside `WORK_STATUS`.

Exit with zero once completion has been sent (it doesn't need to be immediately
after, and superman will not kill the process once completion is received, so
you can do cleanup).
