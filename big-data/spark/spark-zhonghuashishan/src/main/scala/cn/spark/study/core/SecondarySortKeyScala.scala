package cn.spark.study.core


class SecondarySortKeyScala(val first: Int, val second: Int) extends Ordered[SecondarySortKeyScala] with Serializable {
  override def compare(other: SecondarySortKeyScala): Int = {

    if (this.first - other.first != 0) {
      return this.first - other.first
    } else {
      return this.second - other.second
    }
  }
}
