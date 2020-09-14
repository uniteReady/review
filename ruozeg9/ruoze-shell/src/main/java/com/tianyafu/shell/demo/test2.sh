#!/bin/bash

source ~/.bashrc

A="ABC"
B="JEPSON"

if [ ${A} == ${B} ];then
  echo "=="
elif [ ${A} == ABC ]; then
    echo "abc"
else
  echo "!="
fi