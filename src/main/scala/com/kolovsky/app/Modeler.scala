package com.kolovsky.app

import com.kolovsky.graph.Graph
import com.kolovsky.modeler.{BasicCostFunction, CostFunction, PathBasedAssignment, Zone}
import com.typesafe.config.{Config, ConfigUtil, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import spark.jobserver._
import scala.collection.JavaConverters._
/**
  * Created by kolovsky on 2.6.17.
  */
object Modeler extends SparkJob with NamedObjectSupport{
  type ODMRow = (Zone, Array[(Zone, Double)])

  implicit def broadcastPersister[Graph]: NamedObjectPersister[NamedBroadcast[Graph]] = new BroadcastPersister[Graph]
  implicit def rddPersister: NamedObjectPersister[NamedRDD[ODMRow]] = new RDDPersister[ODMRow]
  implicit def objPersister: NamedObjectPersister[NamedObj[Array[Double]]] = new ObjPersister[Array[Double]]

  /**
    * Main method of the job. API:
    * model: model name
    * task: type of the job (load_model, traffic, temp_traffic)
    *   load_model
    *   traffic - need parameter update
    *   temp_traffic
    * @param sc
    * @param config
    * @return
    */
  override def runJob(sc: SparkContext, config: Config): Any = {
    val task = config.getString("task")

    if (task == "load_model"){
      val modelName = config.getString("model")
      loadModel(sc, modelName)
      return "OK"
    }
    if (task == "traffic"){
      return computeTraffic(sc, config)
    }
    if (task == "temp_traffic"){
      return getTempResult(config.getString("model"))
    }
    return false
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    SparkJobValid
  }

  /**
    * Load model from Database to cluster
    * @param sc SparkContext
    * @param name name of the model
    */
  def loadModel(sc: SparkContext, name: String): Unit = {
    val db_url = ConfigFactory.load("modeler.conf").getString("modeler.database.url")
    // Database
    val d = new BasicDatabase(AppConf.getConf().getString("modeler.database.url"), name)

    // Graph persist
    val g = new Graph()
    g.addEdges(d.getEdges())
    g.properties += (("cost", d.getCost()))
    g.properties += (("capacity", d.getCapacity()))

    // Graph persist
    val BC_graph = sc.broadcast(g)
    namedObjects.update(name+":bc:graph", NamedBroadcast(BC_graph))

    //named objects for traffic
    namedObjects.update(name+":traffic", NamedObj(Array(0.0)))

    //ODM persist
    val rowODM = sc.parallelize(d.getODM())
      .map(x => (x._1,Array((x._2, x._3))))
      .reduceByKey(_++_)
    val odm: RDD[ODMRow] = rowODM
    namedObjects.update(name+":rdd:odm", NamedRDD(odm, forceComputation = false, storageLevel = StorageLevel.MEMORY_ONLY))
  }

  /**
    * Assignment ODM using by Path based method
    * @param sc - SparkContext
    * @param config - typesave config
    * @return traffic
    */
  def computeTraffic(sc: SparkContext, config: Config): Any ={
    val modelName = config.getString("model")
    // loads ODM and Graph from cache
    val NamedRDD(odm, _ ,_) = namedObjects.get[NamedRDD[ODMRow]](modelName+":rdd:odm").get
    val NamedBroadcast(bc_graph) = namedObjects.get[NamedBroadcast[Graph]](modelName+":bc:graph").get

    // defines cost function
    val cf = new BasicCostFunction()

    // cost and capacity
    val cost = bc_graph.value.properties("cost").asInstanceOf[Array[Double]].clone()
    val capacity = bc_graph.value.properties("capacity").asInstanceOf[Array[Double]].clone()

    // change Graph
    if (config.hasPath("update")){
      val updateList = config.getObjectList("update").asScala
      for (c <- updateList){
        val index = c.toConfig.getInt("index")
        val newCost = c.toConfig.getDouble("cost")
        val newCapacity = c.toConfig.getDouble("capacity")
        capacity(index) = newCapacity
        cost(index) = newCost
      }
    }

    // callback after every iteration
    def call(traffic: Array[Double]): Unit ={
      namedObjects.update(modelName+":traffic", NamedObj(traffic))
    }

    // create assignment object
    val a = new PathBasedAssignment(bc_graph, cost, cf, capacity, 10, 1.0, 0.0003, call)
    a.run(odm)
  }

  /**
    * Read non final traffic during computation
    * @param modelName name of the model
    * @return non final traffic
    */
  def getTempResult(modelName: String): Any ={
    val NamedObj(traffic) = namedObjects.get[NamedObj[Array[Double]]](modelName+":traffic").get
    traffic
  }

}
