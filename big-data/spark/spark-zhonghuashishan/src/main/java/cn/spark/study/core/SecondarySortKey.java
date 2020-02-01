package cn.spark.study.core;

import scala.Serializable;
import scala.math.Ordered;

import java.util.Objects;

/**
 * 自定义的二次排序key
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    //首先在自定义的 key里面 ，定义需要进行排序的列
    private Integer first;

    private Integer second;

    //其次 为要进行排序的多个列，提供getter和setter方法 以及hashcode和equals方法


    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SecondarySortKey that = (SecondarySortKey) o;
        return Objects.equals(first, that.first) &&
                Objects.equals(second, that.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public int compare(SecondarySortKey other) {
        if (this.first - other.first != 0) {
            return this.first - other.first;
        } else {
            return this.second - other.second;
        }
    }

    @Override
    public int compareTo(SecondarySortKey other) {
        if (this.first - other.first != 0) {
            return this.first - other.first;
        } else {
            return this.second - other.second;
        }
    }

    @Override
    public boolean $less(SecondarySortKey other) {
        if (this.first < other.first) {
            return true;
        } else if (this.first == other.first && this.second < other.second) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey other) {
        if (this.first > other.first) {
            return true;
        } else if (this.first == other.first && this.second > other.second) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey other) {
        if (this.first == other.first && this.second == other.second) {
            return true;
        } else if (this.$less(other)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey other) {
        if (this.first == other.first && this.second == other.second) {
            return true;
        } else if (this.$greater(other)) {
            return true;
        }
        return false;
    }


}
