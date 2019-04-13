package com.salesforce.app

import salesforce.House
import com.salesforce.op._
import com.salesforce.op.readers._
import com.salesforce.op.evaluators._
import com.salesforce.op.features.types._
import com.salesforce.op.stages.impl.classification._
import com.salesforce.op.stages.impl.preparators._
import com.salesforce.op.stages.impl.regression._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object RealEstate extends OpAppWithRunner with Features {

  val randomSeed = 1234

  ////////////////////////////////////////////////////////////////////////////////
  // READER DEFINITIONS
  /////////////////////////////////////////////////////////////////////////////////
  val schema = HousePrediction.getClassSchema

  type Data = Either[RDD[House], Dataset[House]]

  trait TrainTestSplit {
    def isTrain: Boolean

    protected def split(data: Data, weights: Array[Double] = Array(0.9, 0.1)): Data = data match {
      case Left(rdd) =>
        val Array(train, test) = rdd.randomSplit(weights, randomSeed)
        Left(if (isTrain) train else test)
      case Right(ds) =>
        val Array(train, test) = ds.randomSplit(weights, randomSeed)
        Right(if (isTrain) train else test)
    }
  }

  abstract class ReaderWithHeaders
      extends CSVAutoReader[HousePrediction](
        readPath = None,
        headers = Seq.empty,
        recordNamespace = schema.getNamespace,
        recordName = schema.getName,
        key = _.getHouseId.toString
      )
      with TrainTestSplit {
    override def read(params: OpParams)(implicit spark: SparkSession): Data = split(super.read(params))
  }

  abstract class ReaderWithNoHeaders
      extends CSVReader[HousePrediction](
        readPath = None,
        schema = schema.toString,
        key = _.getHouseId.toString
      )
      with TrainTestSplit {
    override def read(params: OpParams)(implicit spark: SparkSession): Data = split(super.read(params))
  }

  class SampleReader(val isTrain: Boolean) extends ReaderWithHeaders

  ////////////////////////////////////////////////////////////////////////////////
  // WORKFLOW DEFINITION
  /////////////////////////////////////////////////////////////////////////////////

  val featureVector =
    Seq(id,date,bedrooms,bathrooms,sqft_living,sqft_lot,floors,waterfront,view,condition,grade,sqft_above,sqft_basement,yr_built,yr_renovated,zipcode,lat,long,sqft_living15,sqft_lot15)
      .transmogrify()

  val label =
    Seq(price)
      .transmogrify()
      .map[RealNN](_.value(0).toRealNN)

  val checkedFeatures = new SanityChecker()
    .setCheckSample(0.10)
    .setInput(label, featureVector)
    .getOutput()

  val pred = RegressionModelSelector()
    .setInput(label, checkedFeatures)
    .getOutput()

  val evaluator =
    Evaluators
      .Regression()
      .setLabelCol(label)
      .setPredictionCol(pred)

  val workflow = new OpWorkflow().setResultFeatures(pred)

  def runner(opParams: OpParams): OpWorkflowRunner =
    new OpWorkflowRunner(
      workflow = workflow,
      trainingReader = new SampleReader(isTrain = true),
      scoringReader = new SampleReader(isTrain = false),
      evaluationReader = Option(new SampleReader(isTrain = false)),
      evaluator = Option(evaluator),
      scoringEvaluator = Option(evaluator)
    )

}