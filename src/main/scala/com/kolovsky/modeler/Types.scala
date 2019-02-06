package com.kolovsky.modeler

import org.apache.spark.rdd.RDD

object Types {
  type ROWODM = RDD[(Zone, Array[(Zone, Double)])]
  type CostM = RDD[(Zone, Array[(Zone, Double)])]
  type ODM = RDD[(Zone, Zone, Double)]

  /**
    * Convert RDD of the ODCells to the Row ODM
    * @param cells cells
    * @return ROWODM
    */
  def cellsToROWODM(cells: RDD[ODCell]): ROWODM = {
    cells.map(c =>{
      (c.s, Array((c.t, c.flow)))
    }).reduceByKey(_++_)
  }

  /**
    * Convert ODM to the Row ODM
    * @param odm ODM
    * @return ROWODM
    */
  def ODMtoROWODM(odm: ODM): ROWODM ={
    odm.map(c => (c._1,Array((c._2, c._3)))).reduceByKey(_++_)
  }
}
