package cn.spark.study.project.dao;


import cn.spark.study.project.domain.Top10Session;

/**
 * top10活跃session的DAO接口
 * @author Administrator
 *
 */
public interface ITop10SessionDAO {

	void insert(Top10Session top10Session);
	
}
