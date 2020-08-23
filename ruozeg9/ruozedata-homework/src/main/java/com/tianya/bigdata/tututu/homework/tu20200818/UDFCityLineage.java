package com.tianya.bigdata.tututu.homework.tu20200818;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

@Description(name = "cityLineage",
        value = "_FUNC_(int id,string name,int parentId,int level) - 返回符合level的city name 的血缘。该方法要求数据是以parentId升序排序的",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(5,沙河镇,3,3) FROM src order by parentId LIMIT 1;\n" + "北京市\t昌平区\t沙河镇")
public class UDFCityLineage extends UDF {
    //key 为 cityId,value为cityNameLineage|cityLevel
    public static  Map<String, String> citiesMap = new HashMap<>();
    /**
     *use ruozedata;
     * create temporary function cityLineage as 'com.tianya.bigdata.tututu.homework.tu20200818.UDFCityLineage';
     *
     * SELECT a .city_name_lineage FROM
     * (SELECT cityLineage (t. id, t. name, t.parentId, 3) city_name_lineage FROM
     * (SELECT id,name,parentId FROM
     * ruozedata.cities ORDER BY parentId) t
     * ) a
     * WHERE a.city_name_lineage IS NOT NULL;
     * @param id
     * @param name
     * @param parentId
     * @param level 希望输出的城市等级
     * @return
     */
    public static String evaluate(Integer id, String name, Integer parentId, Integer level) {
        String key = id < 10 ? "0" + id : "" + id;
        String parentIdKey = parentId < 10 ? "0" + parentId : "" + parentId;
        Integer cityLevel = 0;
        String cityNameLineage = "";
        if("00".equals(parentIdKey)){
            cityNameLineage = name;
            cityLevel = 1;
            citiesMap.put(key,name+"|"+cityLevel);
        }else {
            String parentCityInfo = citiesMap.get(parentIdKey);
            String[] infos = parentCityInfo.split("\\|");
            cityNameLineage = infos[0] + "\t"+ name;
            cityLevel = Integer.valueOf(infos[1])+1;
            citiesMap.put(key,cityNameLineage + "|"+cityLevel);
        }
        return cityLevel.equals(level) ? cityNameLineage:null;
    }

}
