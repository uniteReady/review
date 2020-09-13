#!/bin/bash

A=1
B=2

# 定义C为整数型变量
declare -i C=${A}+1
echo "${C}"

if [ ${A} == ${B} ];then
  echo "=="
elif [ ${A} -gt ${B} ]; then # 数值判断
  echo ">"
elif [ ${C} == 2 ] || [ ${A} != ${B} ]; then
  echo "或"
elif [ ${A} -lt ${B} ] && [ ${C} == ${B} ]; then # 多条件判断
  echo "< && diff 1"
else
  echo "other"
fi
