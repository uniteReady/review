package cn.spark.study.project.dao.impl;

import cn.spark.study.project.dao.ISessionRandomExtractDAO;
import cn.spark.study.project.domain.SessionRandomExtract;
import cn.spark.study.project.jdbc.JDBCHelper;

public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
