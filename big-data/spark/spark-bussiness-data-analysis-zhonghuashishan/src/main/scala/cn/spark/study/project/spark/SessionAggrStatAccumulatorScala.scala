package cn.spark.study.project.spark

import cn.spark.study.project.constant.Constants
import cn.spark.study.project.utils.StringUtils
import org.apache.spark.util.AccumulatorV2

/**
  *
  */
class SessionAggrStatAccumulatorScala extends AccumulatorV2[String,String]{
  private var strValue:String= Constants.ACCUMULATOR_VALUE

  override def isZero: Boolean = strValue==Constants.ACCUMULATOR_VALUE

  override def copy(): AccumulatorV2[String, String] = {
    val newAccumulator = new SessionAggrStatAccumulatorScala()
    newAccumulator.strValue = Constants.ACCUMULATOR_VALUE
    newAccumulator
  }

  override def reset(): Unit = strValue = Constants.ACCUMULATOR_VALUE

  override def add(v: String): Unit = {
    if(StringUtils.isEmpty(v)){
      return
    }
    val oldValue: String = StringUtils.getFieldFromConcatString(strValue,"\\|",v)
    if(!StringUtils.isEmpty(oldValue)){
      val newValue = oldValue.toInt + 1
      StringUtils.setFieldInConcatString(strValue,"\\|",v,String.valueOf(newValue))
    }
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    if(null == other){
      return
    }
    if(other.isInstanceOf[SessionAggrStatAccumulatorScala] ) {
      val another: SessionAggrStatAccumulatorScala = other.asInstanceOf[SessionAggrStatAccumulatorScala]
      val anotherValue: String = another.strValue
      val keyArrays = Array(Constants.SESSION_COUNT, Constants.TIME_PERIOD_1s_3s,
        Constants.TIME_PERIOD_4s_6s, Constants.TIME_PERIOD_7s_9s, Constants.TIME_PERIOD_10s_30s,
        Constants.TIME_PERIOD_30s_60s, Constants.TIME_PERIOD_1m_3m, Constants.TIME_PERIOD_3m_10m,
        Constants.TIME_PERIOD_10m_30m, Constants.TIME_PERIOD_30m, Constants.STEP_PERIOD_1_3,
        Constants.STEP_PERIOD_4_6, Constants.STEP_PERIOD_7_9, Constants.STEP_PERIOD_10_30,
        Constants.STEP_PERIOD_30_60, Constants.STEP_PERIOD_60)

      for (key <- keyArrays) {
        val thisKeyValue: String = StringUtils.getFieldFromConcatString(this.strValue,"\\|",key)
        val anotherKeyValue: String = StringUtils.getFieldFromConcatString(anotherValue,"\\|",key)
        val newValue: String = (thisKeyValue.toInt+ anotherKeyValue.toInt).toString
        StringUtils.setFieldInConcatString(this.strValue,"\\|",key,newValue)
      }

    }
  }

  override def value: String = strValue
}
