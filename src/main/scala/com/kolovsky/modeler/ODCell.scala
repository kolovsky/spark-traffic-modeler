package com.kolovsky.modeler

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kolovsky on 26.8.17.
  */
class ODCell {
  var paths: ArrayBuffer[Path] = ArrayBuffer()
  var flow: Double = 0
  var s: Zone = _
  var t: Zone = _
  var gradient: Double = 0
  var direction: Double = 0
}
