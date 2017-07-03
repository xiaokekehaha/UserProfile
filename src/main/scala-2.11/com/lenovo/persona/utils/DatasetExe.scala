package com.lenovo.persona.utils

import org.apache.spark.sql.Dataset

import scala.collection.{Map, mutable}
import scala.reflect.ClassTag

object DatasetExe {

  implicit class RddExtends[K: ClassTag, V: ClassTag](dataset: Dataset[(K, V)]) {

    def collectAsMap(): Map[K, V] = {
      val data = dataset.collect()
      val map = new mutable.HashMap[K, V]
      map.sizeHint(data.length)
      data.foreach { pair => map.put(pair._1, pair._2) }
      map
    }
  }

}

