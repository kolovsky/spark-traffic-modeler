package com.kolovsky.app
import java.sql.{Connection, DriverManager}

import com.kolovsky.modeler.Zone

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kolovsky on 5.6.17.
  */
class BasicDatabase(uri: String, modelName: String) extends Database {
  override def getEdges(): Array[(Int, Int, Boolean)] = {
    val connection = getConnection()
    connection.setAutoCommit(false)
    val sql = "SELECT source, target, one_direction FROM "+modelName+".edge ORDER BY edge_id;"
    val st = connection.prepareStatement(sql)
    st.setFetchSize(10000)
    val rs = st.executeQuery()
    val edges: ArrayBuffer[(Int, Int, Boolean)] = ArrayBuffer()
    while (rs.next()){
      edges += ((rs.getInt("source"), rs.getInt("target"), rs.getBoolean("one_direction")))
    }
    edges.toArray
  }

  override def getCapacity(): Array[Double] = {
    val connection = getConnection()
    connection.setAutoCommit(false)
    val sql = "SELECT capacity FROM "+modelName+".edge ORDER BY edge_id;"
    val st = connection.prepareStatement(sql)
    st.setFetchSize(10000)
    val rs = st.executeQuery()
    val capacity: ArrayBuffer[Double] = ArrayBuffer()
    while (rs.next()){
      capacity += rs.getDouble("capacity")
    }
    capacity.toArray
  }

  override def getCost(): Array[Double] = {
    val connection = getConnection()
    connection.setAutoCommit(false)
    val sql = "SELECT cost FROM "+modelName+".edge ORDER BY edge_id;"
    val st = connection.prepareStatement(sql)
    st.setFetchSize(10000)
    val rs = st.executeQuery()
    val cost: ArrayBuffer[Double] = ArrayBuffer()
    while (rs.next()){
      cost += rs.getDouble("cost")
    }
    cost.toArray
  }

  override def getODM(): Array[(Zone, Zone, Double)] = {
    val connection = getConnection()
    connection.setAutoCommit(false)
    val sql = "SELECT source, source_node, target, target_node, flow FROM "+modelName+".odm;"
    val st = connection.prepareStatement(sql)
    st.setFetchSize(10000)
    val rs = st.executeQuery()
    val odm: ArrayBuffer[(Zone, Zone, Double)] = ArrayBuffer()
    while (rs.next()){
      val s = new Zone(rs.getInt("source"), rs.getInt("source_node"))
      val t = new Zone(rs.getInt("target"), rs.getInt("target_node"))
      odm += ((s, t, rs.getDouble("flow")))
    }
    odm.toArray
  }
  private def getConnection(): Connection ={
    try{
      Class.forName("org.postgresql.Driver")
      return  DriverManager.getConnection(uri)
    }catch {
      case e => e.printStackTrace
    }
    null
  }
}
