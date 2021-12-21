#!/bin/sh

set -e
set -x

bin_dir="/usr/local/bin"

data_dir="/var/run/tezos"
node_dir="$data_dir/node"
node_data_dir="$node_dir/data"
node="$bin_dir/tezos-node"

exec "${node}" snapshot export --data-dir /var/run/tezos/node/data
