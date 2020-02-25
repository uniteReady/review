package cn.spark.study.project.constant;

/**
 * 常量接口
 * 存放的是键值对中的key
 */
public interface Constants {

    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USERNAME="jdbc.username";
    String JDBC_PASSWORD = "jdbc.password";
    String SPARK_LOCAL = "spark.local";
    /**
     * spark作业相关的常量
     */
    String SPARK_APP_NAME_SESSION="UserVisitSessionAnalyzeSpark";
    String FIELD_SESSION_ID = "sessionId";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_AGE="age";
    String FIELD_PROFESSIONAL="professional";
    String FIELD_CITY="city";
    String FIELD_SEX="sex";
    //访问时长
    String FIELD_VISIT_LENGTH = "visitLength";
    //访问步长
    String FIELD_STEP_LENGTH = "stepLength";
    //session 开始时间
    String FIELD_START_TIME = "startTime";


    String SESSION_COUNT = "session_count";
    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";

    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";


    String ACCUMULATOR_VALUE = Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";
    /**
     * 任务相关的常量
     */
    String PARAM_START_DATE="startDate";
    String PARAM_END_DATE="endDate";
    String PARAM_START_AGE="startAge";
    String PARAM_END_AGE="endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_SEX = "sex";
    String PARAM_KEYWORDS= "keywords";
    String PARAM_CATEGORY_IDS= "categoryIds";

}
