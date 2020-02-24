package cn.spark.study.project.spark;

import cn.spark.study.project.constant.Constants;
import cn.spark.study.project.utils.StringUtils;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;
import java.util.List;

/**
 * spark2.x之后
 * 泛型的第一个参数为输入类型
 * 第二个参数为输出类型
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String,String> {

    String value = Constants.ACCUMULATOR_VALUE;


    /**
     * 判断是否为初始值
     * @return
     */
    @Override
    public boolean isZero() {
        return Constants.ACCUMULATOR_VALUE.equals(value);
    }

    /**
     * 拷贝一个新的AccumulatorV2
     */
    @Override
    public AccumulatorV2<String, String> copy() {
        SessionAggrStatAccumulator accumulator = new SessionAggrStatAccumulator();
        accumulator.value = Constants.ACCUMULATOR_VALUE;
        return accumulator;
    }

    /**
     * 重置累加器中的值
     */
    @Override
    public void reset() {
        value = Constants.ACCUMULATOR_VALUE;
    }

    /**
     * 根据传入的v字段做相应的逻辑计算
     * @param v 这个传的是key  比如: Constants.TIME_PERIOD_1s_3s
     */
    @Override
    public void add(String v) {
        if(StringUtils.isEmpty(v)){
            return;
        }
        String oldValue = StringUtils.getFieldFromConcatString(value, "\\|", v);
        if(oldValue!=null){
            Integer newValue = Integer.valueOf(oldValue) + 1;
            value = StringUtils.setFieldInConcatString(value,"\\|",v,String.valueOf(newValue));
        }
    }

    /**
     * 合并另一个类型相同的累加器;
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if(null == other){
            return;
        }
        if(other instanceof SessionAggrStatAccumulator){
            SessionAggrStatAccumulator another = (SessionAggrStatAccumulator) other;
            String anotherValue = another.value;
            List<String> keyList = Arrays.asList(Constants.SESSION_COUNT, Constants.TIME_PERIOD_1s_3s,
                    Constants.TIME_PERIOD_4s_6s, Constants.TIME_PERIOD_7s_9s, Constants.TIME_PERIOD_10s_30s,
                    Constants.TIME_PERIOD_30s_60s, Constants.TIME_PERIOD_1m_3m, Constants.TIME_PERIOD_3m_10m,
                    Constants.TIME_PERIOD_10m_30m, Constants.TIME_PERIOD_30m, Constants.STEP_PERIOD_1_3,
                    Constants.STEP_PERIOD_4_6, Constants.STEP_PERIOD_7_9, Constants.STEP_PERIOD_10_30,
                    Constants.STEP_PERIOD_30_60, Constants.STEP_PERIOD_60);
            for (String key : keyList) {
                String oldValue = StringUtils.getFieldFromConcatString(this.value, "\\|", key);
                String anotherOldValue = StringUtils.getFieldFromConcatString(anotherValue, "\\|", key);
                String newValue = oldValue;
                if(StringUtils.isNotEmpty(anotherOldValue)){
                    newValue = String.valueOf(Integer.valueOf(oldValue)+Integer.valueOf(anotherOldValue));
                }
                this.value=StringUtils.setFieldInConcatString(value,"\\|",key,newValue);
            }
        }

    }

    /**
     * 返回当前的value
     * @return
     */
    @Override
    public String value() {
        return this.value;
    }
}
