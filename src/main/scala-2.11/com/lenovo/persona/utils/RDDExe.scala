// scalastyle:off
package com.lenovo.persona.utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDExe {

  implicit class RddExtends[T: ClassTag](rdd:RDD[T]) {

    def unions(rdds:RDD[T]*) = {
      val size = rdds.size
      var accumulator = rdd
      for (i <- 0 to size -1)
        accumulator = accumulator.union(rdds(i))
      accumulator
    }

    def zipWithId():RDD[(Long,T)] = {
      rdd.zipWithUniqueId().map( x => (x._2,x._1))
    }
  }

}
