package cn.spark.study.project.test.dao;

import cn.spark.study.project.dao.ITaskDAO;
import cn.spark.study.project.dao.factory.DAOFactory;
import cn.spark.study.project.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());  
	}
	
}
