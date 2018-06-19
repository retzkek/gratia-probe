AWS VM Probe
============

This probe queries the AWS API hourly to count the number of EC2 VM
instances running for the account(s) for which credentials are
provided, in some or all regions.

Installation
------------

The probe should be installed from the [OSG YUM
Repositories](https://opensciencegrid.org/docs/common/yum/).

Configuration
-------------

Probe configuration is in `/etc/gratia/awsvm/ProbeConfig`

### Credentials

AWS credentials should be in `$HOME/.aws/credentials`, for example:

    [default]
    aws_access_key_id = AKIXXXXXXXXXXXXXXCOA
    aws_secret_access_key = bFSXXXXXXXXXXXXXXXXXXXXXXXXXX3ey

If multiple named accounts/profiles are included, each will be
queried in turn, otherwise the `default` profile will be used.

### Instance Types and Prices

AWS maintains a list of EC2 instance types and on-demand prices at:
https://aws.amazon.com/ec2/pricing/on-demand/

This information is required by the probe for correctly including
hardware specs and pricing for each instance; the distribution
includes this data in `/usr/share/gratia/awsvm`, and is configured to
use the included data by default, but it may be out of date.

This distribution includes a script that can attempt to scrape this
data from the AWS "unofficial" API, run it with:

    python -m gratia.awsvm.demand_data

This will output the data in JSON format to stdout. You can
redirect that to a file and change the `HardwareDetailsFile` setting
to point to the updated data.

It it also possible to have the probe download the data every time it
is run, by setting `HardwareDetailsURL`; note that the file will be
used if there is an error loading the data from the URL. However, this
is not recommended, as AWS may change the location and/or content of
the data at any time, so it is advised to manually download and review
the data on a regular basis, or whenever a change is announced in
the [AWS News Blog](https://aws.amazon.com/blogs/aws/).

### Regins

The AWS regions to query for instances can be set as a comma-delimited list
in `AWSRegions`, e.g.

    AWSRegions="us-east-1,us-west-1,us-west-2"

or if left blank the proble will attemp to query the complete region
list from AWS and every region will be queried for running VMs.
