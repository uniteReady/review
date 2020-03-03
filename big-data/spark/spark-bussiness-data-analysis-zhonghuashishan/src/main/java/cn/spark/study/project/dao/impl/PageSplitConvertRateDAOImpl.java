package cn.spark.study.project.dao.impl;

import cn.spark.study.project.dao.IPageSplitConvertRateDAO;
import cn.spark.study.project.domain.PageSplitConvertRate;
import cn.spark.study.project.jdbc.JDBCHelper;

public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,new Object[]{pageSplitConvertRate.getTaskid(),pageSplitConvertRate.getConvertRate()});
    }
}
