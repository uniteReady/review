package cn.spark.study.project.domain;

import java.io.Serializable;

/**
 * top10活跃session
 * @author Administrator
 *
 */
public class Top10Session implements Serializable {

	private long taskid;
	private String categoryid;
	private String sessionid;
	private long clickCount;

	public Top10Session() {
	}

	public Top10Session(long taskid, String categoryid, String sessionid, long clickCount) {
		this.taskid = taskid;
		this.categoryid = categoryid;
		this.sessionid = sessionid;
		this.clickCount = clickCount;
	}

	public long getTaskid() {
		return taskid;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public String getSessionid() {
		return sessionid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public String getCategoryid() {
		return categoryid;
	}

	public void setCategoryid(String categoryid) {
		this.categoryid = categoryid;
	}
}
