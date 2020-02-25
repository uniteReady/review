package cn.spark.study.project.dao.impl;

import cn.spark.study.project.dao.ISessionDetailDAO;
import cn.spark.study.project.domain.SessionDetail;
import cn.spark.study.project.jdbc.JDBCHelper;

public class SessionDetailDAOImpl implements ISessionDetailDAO {

    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{
            sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
