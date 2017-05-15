package com.lenovo.persona.model.labelrdd

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class SVMTrain {

  lazy val numiterations = 100
  lazy val stepSize = 0.01
  lazy val regParam = 0.1
  lazy val batchSize = 1.0

  def process(data: RDD[LabeledPoint]): SVMModel = {

    // 参数需要迭代尝试
    val model = SVMWithSGD.train(data, numiterations, stepSize, regParam, batchSize)
    model
  }

  def verify(data: RDD[LabeledPoint], model: SVMModel): Double = {
    val predictionAndLabel = data.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / data.count()
    accuracy
  }

}
