#!/bin/bash

#         sh standard_shell.sh db table where
#-------------------------------------------------
#FileName:            standard_shell.sh
#Version:             1.0
#Date:                2020-09-14
#Author:              tianyafu
#Description:         example of shell script
#Notes:               project ......等脚本修改日志
#-------------------------------------------------

# 任何脚本开头必须加上 set -u，以后遇到不存在的变量，脚本直接终端运行
set -u

USAGE="Usage : $0 db table where"
[ $# -ne 3 ] && echo "$USAGE" && exit 1

# start

#应用公共变量脚本
source ~/.bashrc
echo "$URL"


echo "www.ruozedata.com"


# end

exit 0