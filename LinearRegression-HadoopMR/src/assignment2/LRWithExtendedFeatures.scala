package assignment2

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.rdd.RDD

/**
 * @brief This class wraps a Logistic Regression Model but
 * automatically add polynomial features (based on existing ones) internally.
 * @param extend A function that specifies how new features are added.
 * @param _tag A string for displaying purpose.
 */
class LRWithExtendedFeatures (extend: Vector => Vector, _tag: String) extends java.io.Serializable{
  val tag = _tag
  val numIterations = 100
  /**
   * Internally, we use a LogisticRegressionModel.
   * But this class doesn't add polynomial features automatically.
   * So we must do it by ourselves before feeding data to it.
   */
  var model: LogisticRegressionModel = null
  
  def extendData(inData: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    inData.map { point => 
      LabeledPoint(point.label, extend(point.features))
    }.cache
  }
  
  def extendAndTrain(trainingData: RDD[LabeledPoint]) = {
    // For sake of accuracy, we use intercept.
    val lrAlg = new LogisticRegressionWithSGD()
    lrAlg.setIntercept(true).optimizer.setNumIterations(numIterations)
    val extendedTrainingData = extendData(trainingData)
    model = lrAlg.run(extendedTrainingData)
  }
  
  def extendAndPredict(inFeatures: Vector): Double = {
    model.predict(extend(inFeatures))
  }

}