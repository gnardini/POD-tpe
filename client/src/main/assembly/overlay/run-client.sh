#!/bin/bash

addresses=192.168.0.13
query=1
inPath=/Users/gnardini/Documents/Code/pod-tpe/census1000000.csv
outPath=out.txt
timeOutPath=time.txt
n=10
prov="Santa Fe"

java -Daddresses=$addresses -Dquery=$query -DinPath=$inPath -DoutPath=$outPath -DtimeOutPath=$timeOutPath -Dn=$n -Dprov="$prov" -cp 'lib/jars/*' "pod.client.Client" $*
