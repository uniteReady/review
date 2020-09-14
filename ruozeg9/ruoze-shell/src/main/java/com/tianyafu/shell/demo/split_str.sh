#!/bin/bash

STR="RZ,JEPSON,XINGXING,HUHU"

OLD_IFS="$IFS"
IFS=","
ARR=($STR)
IFS="$OLD_IFS"

for x in ${ARR[*]}; do
  echo ${x}
done
