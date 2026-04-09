#!/bin/bash

set -x 

threeway="false"
scalable_tput="false"
cfg_dir="cfg"

source $(dirname $0)/common.sh

clients=("1")
num_shards=("1")
msg_size=("4096")

for size in "${msg_size[@]}"; 
do 
    for shard in "${num_shards[@]}";
    do 
        kill_cons_svr
        kill_shard_svrs
        kill_dur_svrs
        kill_clients

        setup_data
        change_num_shards $shard
        change_stripe_unit 1000
        run_shard_svr $shard
        run_dur_svrs
        run_cons_svr

        sudo ../build/src/client/basic_cli \
        -P ../cfg/dl_client.prop \
        -P ../cfg/be.prop \
        -p mode=w \
        -p node_id=0 \
        -p client_id=$c \
        -p count=$2 \
        -p threads=$1 \
        -p ratio=$3

        kill_cons_svr
        kill_shard_svrs
        kill_dur_svrs
        collect_logs
        mkdir -p ${ll_dir}/logs_${c}_${size}_${shard}
        mv $ll_dir/logs/* ${ll_dir}/logs_${c}_${size}_${shard}
        rm -rf $ll_dir/logs
    done
done