package com.kolovsky.app

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by kolovsky on 6.6.17.
  */
object AppConf {
  val confFile = "/home/kolovsky/Dokumenty/traffic-modeler/modeler/modeler.conf"
  //val confFile = "/home/kolovsky/modeler.conf"
  /**
    *
    * @return configuration
    */
  def getConf(): Config = {
    val f = new java.io.File(confFile)
    ConfigFactory.load(ConfigFactory.parseFile(f))
  }

}
