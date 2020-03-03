package cn.spark.study.project.dao.factory;


import cn.spark.study.project.dao.*;
import cn.spark.study.project.dao.impl.*;

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

	public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){return new SessionRandomExtractDAOImpl();}

	public static ISessionDetailDAO getSessionDetailDAO(){return new SessionDetailDAOImpl();}

	public static ITop10CategoryDAO getTop10CategoryDao(){return new Top10CategoryDAOImpl(); }

	public static ITop10SessionDAO getTop10SessionDAO(){return new Top10SessionDAOImpl();}

	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO(){return new PageSplitConvertRateDAOImpl();}
	
}
