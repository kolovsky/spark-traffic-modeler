package com.kolovsky.modeler

/**
  * Created by kolovsky on 24.5.17.
  */
class Zone(zone_id: Int, node_id: Int) extends Serializable{
  val n = node_id
  val id = zone_id

  def compare(that: Zone): Int = {
    this.id - that.id
  }

  override def hashCode: Int = {
    return id.hashCode()
  }

  override def equals(that: Any): Boolean = that match {
    case that: Zone => that.canEqual(this) && this.hashCode == that.hashCode
    case _ => false
  }

  def canEqual(a: Any) = a.isInstanceOf[Zone]
}
