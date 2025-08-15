# wfmeta-darshan

Python module that, given a directory of Darshan logs, reads them into Log objects and aggregates the modules across independent Darshan log files. It then writes these aggregated module statistics into csv files using Pandas DataFrames, as well as a file describing metadata about the collected files themselves.

## Usage

```
usage: wfmeta_darshan [-h] [-d] input output

positional arguments:
  input        Relative directory containing the darshan logs to parse and
               aggregate.
  output       Relative directory to write the aggregated data.

options:
  -h, --help   show this help message and exit
  -d, --debug  If true, prints additional debug messages during runtime.
```