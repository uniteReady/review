package com.tianya.bigdata.hadoop.day20200705;

public class WordCountMapper implements RuozedataMapper {
    @Override
    public void map(String line, RuozedataContext context) {
        String[] splits = line.split(",");

        for (String word : splits) {
            Object value = context.get(word);
            if(null == value){
                //上下文中没有值
                context.write(word,1);
            }else{
                //上下文中有值
                context.write(word,Integer.valueOf(value.toString())+1);
            }

        }
    }
}
