package com.tianya.bigdata.tututu.homework.tu20200826;

public class BigTableJoinBigTableMRDriver {
    /**
     * 大表join大表出现数据倾斜
     *
     * 这里首先要确认导致数据倾斜的key是否有业务含义，如果在业务上没什么含义，直接先过滤掉就行
     *
     * 1.检查导致数据倾斜的key多不多，是否可枚举或者通过数据采样来确定
     * 如果倾斜key明确且数量不多，即可以将其中一张大表分拆，拆成不会导致倾斜的数据和导致倾斜的数据两部分
     * 以此转换为正常join无倾斜+小表join大表的倾斜问题
     * 2.如果导致倾斜的key很多，不可枚举。这个太难了 暂时不会
     */
}
