package com.lenovo.persona.model.labelrdd

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class NBTrain extends Serializable{

  def process(data: RDD[LabeledPoint]): NaiveBayesModel = {

    // 多值分布情况选用multinomial,1.0为默认的平滑系数，解决空值问题
    val model = NaiveBayes.train(data, lambda = 1.0, modelType = "multinomial")
    model
  }

  def verify(data: RDD[LabeledPoint], model: NaiveBayesModel): Double = {
    val predictionAndLabel = data.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / data.count()
    accuracy
  }
}