package cn.spark.study.project.spark;

import scala.Serializable;
import scala.math.Ordered;

/**
 * 品类二次排序key
 *
 * 封装你要进行排序算法需要的几个字段：点击次数、下单次数和支付次数
 * 实现Ordered接口要求的几个方法
 *
 * 跟其他key相比，如何来判定大于、大于等于、小于、小于等于
 *
 * 依次使用三个次数进行比较，如果某一个相等，那么就比较下一个
 *
 * @author Administrator
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {
    private Long clickCount ;
    private Long orderCount ;
    private Long payCount ;


    public CategorySortKey() {
    }

    public CategorySortKey(Long clickCount, Long orderCount, Long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if(this.clickCount > other.clickCount){
            return true;
        }else if(this.clickCount == other.clickCount && this.orderCount > other.orderCount){
            return true;
        }else if(this.clickCount == other.clickCount && this.orderCount == other.orderCount && this.payCount > other.payCount){
            return true;
        }
        return false;
    }
    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if($greater(other)){
            return true;
        }else if(this.clickCount == other.clickCount && this.orderCount == other.orderCount && this.payCount == other.payCount){
            return  true;
        }
        return false;
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if(this.clickCount < other.clickCount){
            return true;
        }else if(this.clickCount == other.clickCount && this.orderCount < other.orderCount){
            return true;
        }else if(this.clickCount == other.clickCount && this.orderCount == other.orderCount && this.payCount < other.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if($less(other)){
            return true;
        }else if (this.clickCount == other.clickCount && this.orderCount == other.orderCount && this.payCount == other.payCount){
            return true;
        }
        return false;
    }


    @Override
    public int compare(CategorySortKey other) {
        if(this.clickCount - other.getClickCount() != 0){
            return (int)(this.clickCount - other.getClickCount());
        }else if (this.orderCount - other.getOrderCount()!=0){
            return (int)(this.orderCount - other.getOrderCount());
        }else if (this.payCount - other.getPayCount() != 0){
            return (int)(this.payCount - other.getPayCount());
        }
        return 0;
    }

    /**
     * 逻辑实现与compare的是一模一样的
     * @param other
     * @return
     */
    @Override
    public int compareTo(CategorySortKey other) {
        if(this.clickCount - other.getClickCount() != 0){
            return (int)(this.clickCount - other.getClickCount());
        }else if (this.orderCount - other.getOrderCount()!=0){
            return (int)(this.orderCount - other.getOrderCount());
        }else if (this.payCount - other.getPayCount() != 0){
            return (int)(this.payCount - other.getPayCount());
        }
        return 0;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Long orderCount) {
        this.orderCount = orderCount;
    }

    public Long getPayCount() {
        return payCount;
    }

    public void setPayCount(Long payCount) {
        this.payCount = payCount;
    }
}
