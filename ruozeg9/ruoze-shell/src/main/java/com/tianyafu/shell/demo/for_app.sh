#!/bin/bash

source ~/.bashrc

for x in $*
do
  echo "${x}"
done


for (( i = 0; i < 10; i++ )); do
    echo "${i}"
done