```
sed主要用来做替换

[root@bbi test]# cat t.log 
a b c
1 2 3
[root@bbi test]# sed -i 's/a/aa/' t.log
[root@bbi test]# cat t.log 
aa b c
1 2 3

# 全局替换
[root@bbi test]# cat t.log 
aa b c
1 2a 3a
[root@bbi test]# sed -i 's/a/w/g' t.log
[root@bbi test]# cat t.log 
ww b c
1 2w 3w
# 全局替换转义字符
[root@bbi test]# cat t.log 
w/w b c
1 2w 3w
[root@bbi test]# sed -i 's/\//aaa/g' t.log 
[root@bbi test]# cat t.log 
waaaw b c
1 2w 3w


```