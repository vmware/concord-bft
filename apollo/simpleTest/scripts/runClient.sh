#!/bin/bash

set -e

scriptdir=$(cd $(dirname $0); pwd -P)

$scriptdir/../client
