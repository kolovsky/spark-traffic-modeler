package com.kolovsky.modeler

import org.apache.spark.rdd.RDD

object Types {
  type ROWODM = RDD[(Zone, Array[(Zone, Double)])]

  def cellsToROWODM(cells: RDD[ODCell]): ROWODM = {
    cells.map(c =>{
      (c.s, Array((c.t, c.flow)))
    }).reduceByKey(_++_)
  }
}
