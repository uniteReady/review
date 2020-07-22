package com.tianya.bigdata.homework.day20200719;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF作业
 * (1)、获取url中的questionId，求Top10
 * (2)、获取课程名称,如果是父子课程的，则用_将父子课程的课程号拼接起来,求Top10
 *
 *[hadoop@hadoop01 hadoop_tmp]$ hdfs dfs -mkdir /ruozedata/hive/question
 * [hadoop@hadoop01 hadoop_tmp]$ hdfs dfs -mkdir /ruozedata/hive/course
 *
 * create external table ruozedata.question(
 * question_url string comment '问答对url'
 * )comment '20200719UDF作业一：问答对TOPN' row format delimited fields terminated by '|' location '/ruozedata/hive/question';
 *
 * create external table ruozedata.course(
 * course_url string comment '课程url'
 * )comment '20200719UDF作业二：课程TOPN' row format delimited fields terminated by '|' location '/ruozedata/hive/course';
 *
 * create temporary function topN as 'com.tianya.bigdata.homework.day20200719.UDFUrlTopN';
 *
 * select a.question_id,a.cnt from (select topN(question_url) as question_id,count(*) as cnt from question group by topN(question_url)) a order by cnt desc limit 10;
 *
 * select a.course_no,a.cnt from (select topN(course_url) as course_no,count(*) as cnt from course group by topN(course_url)) a order by cnt desc limit 10;
 */
public class UDFUrlTopN extends UDF {

    public static final String SUFFIX = ".html";

    public static final  String SPLIT_STR = "/";

    public static final String SEPARATOR = "_";


    public String evaluate(String input) {
        if(null == input){
            return null;
        }
        if(input.contains(SUFFIX)){
            input = input.substring(0,input.indexOf(SUFFIX));
        }
        return getResultStr(input);

    }

    /**
     * 获取最终结果，如果是父子课程，则用_拼接
     * @param input
     * @return
     */
    public String getResultStr(String input){
        String resultStr = "";
        String[] splits = input.split(SPLIT_STR);

        for (String str : splits) {
            if(null != str && !"".equals(str) && isNumber(str)){
                resultStr += str+SEPARATOR;
            }
        }
        //将最后的_去掉
        if(resultStr.endsWith(SEPARATOR)){
            resultStr = resultStr.substring(0,resultStr.length() - 1);
            return resultStr;
        }else{
            //整个url中不带数字
            return null;
        }

    }

    /**
     * 判断一个字符串是否为数字类型
     * @param str
     * @return
     */
    private boolean isNumber(String str){
        String reg = "^[0-9]*$";
        return str.matches(reg);
    }


    /*public static void main(String[] args) {
//        String input = "http://ruozedata.com/question/4515a454/2a.html?key1=value1&key2=value2";
        String input = "http://ruozedata.com/question/4515454/2";

        String result = evaluate(input);
        System.out.println(result);
    }*/
}
