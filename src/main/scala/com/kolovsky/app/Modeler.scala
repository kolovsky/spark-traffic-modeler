package com.kolovsky.app

import com.kolovsky.graph.Graph
import com.kolovsky.modeler._
import com.typesafe.config.{Config, ConfigRenderOptions}
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
    * @param sc SparkContext
    * @param config config
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
      return getTempResult(config.getString("model"), config.getString("comp_id"))
    }
    false
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
    val db_url = AppConf.db_url

    // Database
    val d = new BasicDatabase(db_url, name)

    // Graph persist
    val g = new Graph()
    g.addEdges(d.getEdges())
    g.addTurnRestriction(d.getTurnRestriction())
    g.properties += (("cost", d.getCost()))
    g.properties += (("capacity", d.getCapacity()))

    // Graph persist
    val BC_graph = sc.broadcast(g)
    namedObjects.update(name+":bc:graph", NamedBroadcast(BC_graph))

    //named objects for traffic
    //namedObjects.update(name+":traffic", NamedObj(Array(0.0)))

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
    // comp_id
    val comp_id = config.getString("comp_id")

    // loads ODM and Graph from cache
    val NamedRDD(odm, _ ,_) = namedObjects.get[NamedRDD[ODMRow]](modelName+":rdd:odm").get
    val NamedBroadcast(bc_graph) = namedObjects.get[NamedBroadcast[Graph]](modelName+":bc:graph").get

    // defines cost function
    val cf = new BasicCostFunction(0.15, 4)

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
      namedObjects.update(modelName+":traffic:"+comp_id, NamedObj(traffic))
    }

    // create assignment object
    //val a = new PathBasedAssignment(bc_graph, cost, cf, capacity, 10, 1.0, 0.0003, call)
    val a = new BAssignment(bc_graph, cost, cf, capacity, 10, 0.001, call, debug = false)
    val out = a.run(odm)
    // delete temp traffic object
    deleteTempTrafficObject(modelName, comp_id)

    // save traffic to database
    if (config.hasPath("cache_name")){
      val d = new BasicDatabase(AppConf.db_url, modelName)
      var result_str = "["
      for(x <- out){
        result_str += x + ", "
      }
      result_str = result_str.substring(0, result_str.length - 2)
      result_str += "]"

      val config_str = config.withOnlyPath("update")
        .withFallback(config.withOnlyPath("comp_id"))
        .withFallback(config.withOnlyPath("model"))
        .withFallback(config.withOnlyPath("task"))
        .withFallback(config.withOnlyPath("cache_name"))
        .resolve().root().render(ConfigRenderOptions.concise())

      d.saveResult(modelName, config.getString("cache_name"), config_str, result_str)
    }
    out
  }

  /**
    * Read non final traffic during computation
    * @param modelName name of the model
    * @param comp_id computation id
    * @return non final traffic
    */
  def getTempResult(modelName: String, comp_id: String): Any ={
    val NamedObj(traffic) = namedObjects.get[NamedObj[Array[Double]]](modelName+":traffic:"+comp_id).get
    traffic
  }

  def deleteTempTrafficObject(modelName: String, comp_id: String): String ={
    val b = namedObjects.getNames().toList.contains(modelName+":traffic:"+comp_id)
    if (b){
      namedObjects.forget(modelName+":traffic:"+comp_id)
      "OK"
    }
    else{
      "DO_NOT_EXISTS"
    }

  }

}
