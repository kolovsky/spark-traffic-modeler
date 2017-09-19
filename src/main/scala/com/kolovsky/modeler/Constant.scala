package com.kolovsky.modeler

import org.apache.spark.rdd.RDD

object Constant {
  type ROWODM = RDD[(Zone, Array[(Zone, Double)])]
}
