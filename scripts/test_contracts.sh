#!/bin/sh
set -eu
exec ruby "$(dirname "$0")/test_contracts.rb"
