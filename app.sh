#!/usr/bin/env sh
#
# This script is run by OpenShift's s2i. Here we guarantee that we run desired
# command
#

if [ "$SUBCOMMAND" = "producer" ]
then
    exec faust --debug --loglevel debug -A advise_reporter main
elif [ "$SUBCOMMAND" = "consumer" ]
then
    exec faust --debug --loglevel debug -A advise_reporter_consumer worker
fi
