#!/bin/bash

# $HM_HOME is needed 
temp_cur_dir=`dirname $0`
cur_full_path=`cd $temp_cur_dir;pwd`
hm_path=${cur_full_path/'/test'/''} 

hm_home=${HM_HOME:-"$hm_path"} # if HM_HOME is not set, use $hm_path

scpath=$hm_home"/scripts"

cd $hm_home

usage () {
    echo "Usage(stop ):${0} [-s(start)|-q(stop)]" >&2
    exit 2
}


while getopts 'sq' flag
do
    case $flag in
        q) opt_stop="1"
        ;;
        s) opt_start="1"
        ;;
        ?) usage
        ;;
    esac
done

hm_stop=${opt_stop:+"stop"}
hm_start=${opt_start:+"start"}

if [ -z "$hm_start" -a -z "$hm_stop" ]; then
        usage
        exit 2
fi
if [ "$hm_start" = "start" -a "$hm_stop" = "stop" ]; then
        usage
        exit 2
fi

if [ "$hm_start" = 'start' ]; then
    $scpath/hm -t create -n foo -s node_foo@ubu -d 
    $scpath/hm -t join -r foo -n bar  -s node_bar@ubu -w node_foo@ubu -d
    $scpath/hm -t join -r foo -n hoge -s node_hoge@ubu -w node_foo@ubu -d
    $scpath/hm -t join -r foo -n cat  -s node_cat@ubu -w node_foo@ubu -d
    $scpath/hm -t join -r foo -n dog  -s node_dog@ubu -w node_foo@ubu -d
fi

if [ "$hm_stop" = 'stop' ]; then
    $scpath/hm -q -w node_foo@ubu -s stop_temp_testing@ubu -d
fi
epmd_path=`which epmd`
EPMD_PATH=`dirname $epmd_path`
$EPMD_PATH/epmd -names

