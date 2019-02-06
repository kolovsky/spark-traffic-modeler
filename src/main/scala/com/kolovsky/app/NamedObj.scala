package com.kolovsky.app
import spark.jobserver.{NamedObject, NamedObjectPersister}

/**
  * Created by kolovsky on 24.8.17.
  */

/**
  * wrapper for named objects of type T
  */
case class NamedObj[T](obj: T) extends NamedObject

/**
  * implementation of a NamedObjectPersister for T objects
  */
class ObjPersister[T] extends NamedObjectPersister[NamedObj[T]] {
  override def persist(namedObj: NamedObj[T], name: String) {
  }

  override def unpersist(namedObj: NamedObj[T]) {
  }

  override def refresh(namedObj: NamedObj[T]): NamedObj[T] = namedObj match {
    case NamedObj(broadcast) =>
      namedObj
  }
}
