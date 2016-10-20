Test script and data for SLURM probe
====================================

This directory contains an export of 20 test jobs from a SLURM 2.6pre4
accounting database for a cluster named "slurmtest1".

* IDs range from 200561 to 200580
* Total CPU hours should be 1720
* Jobs vary in core count
* Some jobs have been preempted and resumed

Data was converted to SLURM 15.08 accounting format for a cluster named
"slurmtest2".

## Using the command-line test tool to summarize CPU hours per day:

### For SLURM accounting schema < 15.08:

    $ ./slurm_meter_test -p password -D database -c slurmtest1 -s 0 --schema 1
    Retrieving jobs completed after 1970-01-01T00:00:00
    Date            Jobs    Hours
    ----------      ------- -------
    2013-07-05      7       14
    2013-07-06      8       166
    2013-07-07      5       1539
    ----------      ------- -------
    total           20      1720

### For SLURM accounting schema >= 15.08:

    $ ./slurm_meter_test -p password -D database -c slurmtest2 -s 0 --schema 2
    Retrieving jobs completed after 1970-01-01T00:00:00
    Date            Jobs    Hours
    ----------      ------- -------
    2013-07-05      7       14
    2013-07-06      8       166
    2013-07-07      5       1539
    ----------      ------- -------
    total           20      1720
