package com.salesforce.app

import salesforce.HousePredictionPrediction
import com.salesforce.op.features.{FeatureBuilder => FB}
import com.salesforce.op.features.types._
import FeatureOps._

trait Features extends Serializable {

  val price = FB
    .Real[HousePrediction]
    .extract(_.getprice.toReal)
    .asResponse

  val id = FB
    .Real[HousePrediction]
    .extract(_.getid.toReal)
    .asPredictor

  val date = FB
    .Real[HousePrediction]
    .extract(_.getdate.toReal)
    .asPredictor

  val bedrooms = FB
    .Real[HousePrediction]
    .extract(_.getbedrooms.toReal)
    .asPredictor

  val bathrooms = FB
    .Real[HousePrediction]
    .extract(_.getbathrooms.toReal)
    .asPredictor

  val sqft_living = FB
    .Real[HousePrediction]
    .extract(_.getsqft_living.toReal)
    .asPredictor

  val sqft_lot = FB
    .Real[HousePrediction]
    .extract(_.getsqft_lot.toReal)
    .asPredictor

  val floors = FB
    .Real[HousePrediction]
    .extract(_.getfloors.toReal)
    .asPredictor

  val waterfront = FB
    .Real[HousePrediction]
    .extract(_.getwaterfront.toReal)
    .asPredictor
	
  val view = FB
    .Real[HousePrediction]
    .extract(_.getview.toReal)
    .asPredictor

  val condition = FB
    .Real[HousePrediction]
    .extract(_.getcondition.toReal)
    .asPredictor

  val grade = FB
    .Real[HousePrediction]
    .extract(_.getgrade.toReal)
    .asPredictor

  val sqft_above = FB
    .Real[HousePrediction]
    .extract(_.getsqft_above.toReal)
    .asPredictor

  val sqft_basement = FB
    .Real[HousePrediction]
    .extract(_.getsqft_basement.toReal)
    .asPredictor

  val yr_built = FB
    .Real[HousePrediction]
    .extract(_.getyr_built.toReal)
    .asPredictor

  val yr_renovated = FB
    .Real[HousePrediction]
    .extract(_.getyr_renovated.toReal)
    .asPredictor
	
  val zipcode = FB
    .Real[HousePrediction]
    .extract(_.getzipcode.toReal)
    .asPredictor

  val lat = FB
    .Real[HousePrediction]
    .extract(_.getlat.toReal)
    .asPredictor

  val long = FB
    .Real[HousePrediction]
    .extract(_.getlong.toReal)
    .asPredictor

  val sqft_living15 = FB
    .Real[HousePrediction]
    .extract(_.getsqft_living15.toReal)
    .asPredictor

  val sqft_lot15 = FB
    .Real[HousePrediction]
    .extract(_.getsqft_lot15.toReal)
    .asPredictor

}

object FeatureOps {
  def asPickList[T](f: T => Any): T => PickList = x => Option(f(x)).map(_.toString).toPickList
}