package cn.spark.study.project.domain;

/**
 * top10品类
 * @author Administrator
 *
 */
public class Top10Category {

	private long taskid;
	private String categoryid;
	private long clickCount;
	private long orderCount;
	private long payCount;

	public Top10Category() {
	}

	public Top10Category(long taskid, String categoryid, long clickCount, long orderCount, long payCount) {
		this.taskid = taskid;
		this.categoryid = categoryid;
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}

	public long getTaskid() {
		return taskid;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}

	public String getCategoryid() {
		return categoryid;
	}

	public void setCategoryid(String categoryid) {
		this.categoryid = categoryid;
	}

	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	public long getOrderCount() {
		return orderCount;
	}
	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}
	public long getPayCount() {
		return payCount;
	}
	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}
	
}
