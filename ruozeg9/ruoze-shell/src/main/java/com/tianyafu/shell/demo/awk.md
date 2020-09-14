```
awk主要是用来取行、取列、取单元格

[root@bbi test]# cat t.log 
a b c
1 2 3

# 取第1列
[root@bbi test]# cat t.log | awk '{print $1}'
a
1
# 取第1第2列
[root@bbi test]# cat t.log | awk '{print $1,$2}'
a b
1 2
# 取第1第2列拼接
[root@bbi test]# cat t.log | awk '{print $1$2}' 
ab
12
# 取行数=1的
[root@bbi test]# cat t.log | awk 'NR==1{print}'
a b c
# 取行数<2的
[root@bbi test]# cat t.log | awk 'NR<2{print}' 
a b c
# 取行数<=2的
[root@bbi test]# cat t.log | awk 'NR<=2{print}'
a b c
1 2 3
# 取第二行的第一列
[root@bbi test]# cat t.log | awk 'NR==2{print $1}'
1

```