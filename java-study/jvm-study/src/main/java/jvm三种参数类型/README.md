# jvm三种类型的参数

## JVM参数类型
```
1） 标准：稳定的
2） X：相对变化较少的
3） XX：jvm调优的重点

1） 标准参数类型 是随着jdk版本升级但基本不会变化的参数 比较稳定 比如 -version  -help等等
2） X参数类型 
3） XX参数类型是调优的重点
    
```

### XX类型参数
```
分为2大类：boolean类型和非boolean类型
a) boolean  格式： -XX:[+/-] name   name为参数名  +为启用  -为禁用
例如：
-XX:+UseG1GC  -XX:-UseG1GC  -XX:+PrintGCDetails
可以使用 jinfo -flag name 进程pid号 来查看相应的参数是否开启 pid可以通过jps来获取
例如：
jinfo -flag PrintGCDetails 21846

b) 非boolean  格式： -XX:name=value
例如：
-XX:MetaspaceSize=21807104  -XX:MaxTenuringThreshold=15
```

### jinfo参数详解
```
直接 jinfo 就可以出来jinfo的帮助文档
Usage:
    jinfo [option] <pid>
        (to connect to running process)
    jinfo [option] <executable <core>
        (to connect to a core file)
    jinfo [option] [server_id@]<remote server IP or hostname>
        (to connect to remote debug server)

where <option> is one of:
    -flag <name>         to print the value of the named VM flag
    -flag [+|-]<name>    to enable or disable the named VM flag
    -flag <name>=<value> to set the named VM flag to the given value
    -flags               to print VM flags
    -sysprops            to print Java system properties
    <no option>          to print both of the above
    -h | -help           to print this help message
    
常用：    
jinfo -flag name  pid
jinfo -flags pid

```

### printFlags系列
```
-XX:+printFlagsInitial
-XX:+printFlagsFinal

java -XX:+PrintFlagsInitial

= 默认值
:= 修改过的

```

### 几个特殊的XX参数
```
-Xms：堆的最小值 InitialHeapSize的缩写  默认值是总内存的1/64
-Xmx: 堆的最大值 MaxHeapSize的缩写  默认值是总内存的1/4
-Xss: 线程栈的大小 ThreadStackSize的缩写
-XX:SurvivorRatio=8 这个是新生代中eden区和survivor区的比例 即2*survivor:eden=2:8  即默认是8:1:1
-XX:NewRatio=2 这个是老年代和新生代的比例 即新生代:老年代=1:2  即新生代占1/3 老年代占2/3
-XX:MaxTenuringThreshold=15 这个是新生代对象的年龄达到15后会转入老年代
```