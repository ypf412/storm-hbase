#!/bin/sh
date='2012-8-7 11:57:00'
base=`date -d "$date" +'%s'`
interval=180
for i in `seq 1 $interval`; do
	((shard=$RANDOM%32))
	((ts=$base + $i))
	((id=$RANDOM%10))
	echo $shard$tsuser$idleftkey$idvalue$id
done
