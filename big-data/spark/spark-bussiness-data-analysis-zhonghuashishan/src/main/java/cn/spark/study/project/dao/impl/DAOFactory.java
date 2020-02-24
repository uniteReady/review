package cn.spark.study.project.dao.impl;


import cn.spark.study.project.dao.ISessionAggrStatDAO;
import cn.spark.study.project.dao.ITaskDAO;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {

	/**
	 * 获取任务管理DAO
	 * @return DAO
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	/**
	 * 获取任务管理DAO
	 * @return DAO
	 */
	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
}
