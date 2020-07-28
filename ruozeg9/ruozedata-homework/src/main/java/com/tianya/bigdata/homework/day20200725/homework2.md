# 翻译

## 总纲
```
In this blog post, we introduce the new window function feature that was added in Apache Spark.
 Window functions allow users of Spark SQL to calculate results such as the rank of a given row or a moving average over a range of input rows. 
 They significantly improve the expressiveness of Spark’s SQL and DataFrame APIs. 
 This blog will first introduce the concept of window functions and then discuss how to use them with Spark SQL and Spark’s DataFrame API.
本文将先介绍窗口函数的概念，然后介绍如何在Spark SQL和DataFrame API 中使用
```

## 什么是窗口函数
```
Before 1.4, there were two kinds of functions supported by Spark SQL that could be used to calculate a single return value. 
Built-in functions or UDFs, such as substr or round, take values from a single row as input, and they generate a single return value for every input row.
 Aggregate functions, such as SUM or MAX, operate on a group of rows and calculate a single return value for every group.
在Spark 1.4版本之前，Spark提供了2种函数来计算单个返回值，一种是内置函数或者UDF，一种是聚合函数

While these are both very useful in practice,
 there is still a wide range of operations that cannot be expressed using these types of functions alone. 
Specifically, there was no way to both operate on a group of rows while still returning a single value for every input row. 
This limitation makes it hard to conduct various data processing tasks like calculating a moving average,
 calculating a cumulative sum, or accessing the values of a row appearing before the current row. 
 Fortunately for users of Spark SQL, window functions fill this gap.
这两类函数虽然在实践中很有用，但在某些场景中却无能为力。比如，对一组数据进行操作，每行数据返回一个值等场景。窗口函数就是用来处理这些场景的

At its core, a window function calculates a return value for every input row of a table based on a group of rows, called the Frame.
 Every input row can have a unique frame associated with it. 
 This characteristic of window functions makes them more powerful than other functions 
 and allows users to express various data processing tasks that are hard (if not impossible) to be expressed without window functions in a concise way. 
 Now, let’s take a look at two examples.
这里提出一个概念，帧，窗口函数是基于一组数据来进行计算的，这组数据就是帧。

We want to answer two questions:

1.What are the best-selling and the second best-selling products in every category?
2.What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product?

To answer the first question “What are the best-selling and the second best-selling products in every category?”, 
we need to rank products in a category based on their revenue, and to pick the best selling and the second best-selling products based the ranking. 
回答第一个问题，我们需要在同一个分类中的数据按照收入进行排序，然后挑出排序为前2的

Without using window functions, it is very hard to express the query in SQL, 
and even if a SQL query can be expressed, it is hard for the underlying engine to efficiently evaluate the query.
如果没有窗口函数，这个查询sql会很难写，即使写出来了，底层的引擎也很难高效的查询

For the second question “What is the difference between the revenue of each product 
and the revenue of the best selling product in the same category as that product?”, to calculate the revenue difference for a product, 
we need to find the highest revenue value from products in the same category for each product.
为了回答第2个问题，我们需要先找到这个分类中的最高收入，然后拿每个收入跟这个最高收入进行比较

Without using window functions, users have to find all highest revenue values of all categories 
and then join this derived data set with the original productRevenue table to calculate the revenue differences
如果没有窗口函数，我们需要找到每个分类各自的最高收入，然后关联上源数据来计算差值

```
## 使用窗口函数
```
Spark SQL supports three kinds of window functions: ranking functions, analytic functions, and aggregate functions. 
The available ranking functions and analytic functions are summarized in the table below.
 For aggregate functions, users can use any existing aggregate function as a window function
Spark SQL 支持3种窗口函数，排序函数，分析函数和聚合函数

To use window functions, users need to mark that a function is used as a window function by either
	1.Adding an OVER clause after a supported function in SQL, e.g. avg(revenue) OVER (...); or
	2.Calling the over method on a supported function in the DataFrame API, e.g. rank().over(...).
使用窗口函数，我们需要在函数后面加上over子句或者使用over相关的API

Once a function is marked as a window function, the next key step is to define the Window Specification associated with this function. 
A window specification defines which rows are included in the frame associated with a given input row. 
A window specification includes three parts:

	1.Partitioning Specification: controls which rows will be in the same partition with the given row. 
	Also, the user might want to make sure all rows having the same value for  the category column are collected to the same machine before ordering 
	and calculating the frame.  
	If no partitioning specification is given, then all data must be collected to a single machine.
	2.Ordering Specification: controls the way that rows in a partition are ordered, 
	determining the position of the given row in its partition.
	3.Frame Specification: states which rows will be included in the frame for the current input row, 
	based on their relative position to the current row.  
	For example, “the three rows preceding the current row to the current row” describes a frame including the current input row 
	and three rows appearing before the current row.
在某个函数后开窗，下一步就是指定一些窗口规范来说明哪些行会进入数据帧，包括以下3部分
    1.设置哪些数据会分在同一个分区
    2.设置排序规则
    3.根据数据与当前行的位置关系来确定是否在数据帧中
举个栗子，当前行的前3行到当前行表示该数据帧有4行数据

In SQL, the PARTITION BY and ORDER BY keywords are used to specify partitioning expressions for the partitioning specification, 
and ordering expressions for the ordering specification, respectively. The SQL syntax is shown below.

OVER (PARTITION BY ... ORDER BY ...)
在SQL中，用PARTITION BY来对数据进行分区，用ORDER BY来对数据进行排序

In addition to the ordering and partitioning, users need to define the start boundary of the frame, 
the end boundary of the frame, and the type of the frame, which are three components of a frame specification.

There are five types of boundaries, 
which are UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING, CURRENT ROW, <value> PRECEDING, and <value> FOLLOWING. 
UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING represent the first row of the partition and the last row of the partition, respectively.
 For the other three types of boundaries, 
 they specify the offset from the position of the current input row and their specific meanings are defined based on the type of the frame. 
 There are two types of frames, ROW frame and RANGE frame.
 
我们可以定义数据帧的开始和结束边界，以及数据帧的类型，这个是数据帧的3种规范
边界有5种类型：无开始边界、无结束边界、当前行、之前的多少行、之后的多少行
无开始边界、无结束边界分别表示从分区的第一行开始计算和从分区的最后一行为边界
其他3种边界表示与当前行的迁移位置，而这个位置信息是根据数据帧的类型来的，有2种类型，分别是ROW frame and RANGE frame.

```

## ROW frame
```
ROW frames are based on physical offsets from the position of the current input row, 
which means that CURRENT ROW, <value> PRECEDING, or <value> FOLLOWING specifies a physical offset.
 If CURRENT ROW is used as a boundary, it represents the current input row.
 <value> PRECEDING and <value> FOLLOWING describes the number of rows appear before and after the current input row, respectively. 
 The following figure illustrates a ROW frame with a 1 PRECEDING as the start boundary and 1 FOLLOWING as the end boundary (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING in the SQL syntax).
ROW frame是数据与当前行的的物理偏移量
```

## RANGE frame
```
RANGE frames are based on logical offsets from the position of the current input row, and have similar syntax to the ROW frame. 
A logical offset is the difference between the value of the ordering expression of the current input row 
and the value of that same expression of the boundary row of the frame.
 Because of this definition, when a RANGE frame is used, only a single ordering expression is allowed.  Also, for a RANGE frame,
 all rows having the same value of the ordering expression with the current input row are considered as same row as far as the boundary calculation is concerned.

Now, let’s take a look at an example. In this example, the ordering expressions is revenue; 
the start boundary is 2000 PRECEDING; and the end boundary is 1000 FOLLOWING (
this frame is defined as RANGE BETWEEN 2000 PRECEDING AND 1000 FOLLOWING in the SQL syntax). 
The following five figures illustrate how the frame is updated with the update of the current input row. 
Basically, for every current input row, based on the value of revenue, 
we calculate the revenue range [current revenue value - 2000, current revenue value + 1000].
 All rows whose revenue values fall in this range are in the frame of the current input row.

RANGE frame 是数据与当前行的逻辑偏移量。逻辑偏移量是当前行与边界范围值内的行之间的偏移量。因此，当使用RANGE frame时，只能允许使用一个排序表达式。
同样的，对于RANGE frame来说，与当前行有相同排序值的行被认为是有相同的偏移量

下面，通过一个例子来说明，排序字段是收入，开始边界是当前行的收入值减2000，结束边界是当前行的收入值加1000。下面展示随着当前行的移动，数据帧的范围也跟随着变化。
对每一行数据的收入来说，我们通过计算[当前收入-2000,当前收入+1000]来确定范围，所有符合该范围的行都是这个数据帧里面的数据

In summary, to define a window specification, users can use the following syntax in SQL.

OVER (PARTITION BY ... ORDER BY ... frame_type BETWEEN start AND end)

Here, frame_type can be either ROWS (for ROW frame) or RANGE (for RANGE frame); 
start can be any of UNBOUNDED PRECEDING, CURRENT ROW, <value> PRECEDING, and <value> FOLLOWING; 
and end can be any of UNBOUNDED FOLLOWING, CURRENT ROW, <value> PRECEDING, and <value> FOLLOWING.

我们可以用以下语法来定义开窗规则
OVER (PARTITION BY ... ORDER BY ... frame_type BETWEEN start AND end)
```

