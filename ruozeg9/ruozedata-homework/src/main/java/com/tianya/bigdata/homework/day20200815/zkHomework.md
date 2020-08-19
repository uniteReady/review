```
0823讲解
1) ls vs ls2 vs stat
2) 请使用curator来实现永久监听
3) 原生API
	级联创建node  a/b/c
	递归删除      a/...  
```

```

stat path [watch]
ls path [watch]
ls2 path [watch]

查看时的区别：
stat path 只展示path的节点信息
ls path   会展示path下的子znode
ls2 path  会展示path下的子znode以及path的节点信息

监听单znode
stat path watch 能监听到set 和delete操作
ls path watch   能监听到delete 和 create 子节点 操作
ls2 path watch  能监听到delete 和 create 子节点 操作

监听有子节点的znode
stat path watch 能监听到create 子节点和 set自己的数据的操作
ls path watch   能监听到create 子节点和delete子节点的操作
ls2 path watch  能监听到create 子节点和delete子节点的操作


```