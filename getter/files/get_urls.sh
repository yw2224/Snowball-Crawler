#!/bin/sh
LEN=$(redis-cli -h 10.172.136.41 -p 30001 LLEN snowball.updatenews)
echo "$LEN"
for i in $(seq 0 $LEN)
do	
	redis-cli -h 10.172.136.41 -p 30001 LINDEX snowball.updatenews $i >> url.data
	echo $i
done
