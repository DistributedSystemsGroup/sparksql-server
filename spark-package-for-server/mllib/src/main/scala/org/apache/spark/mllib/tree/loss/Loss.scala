/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.tree.loss

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.TreeEnsembleModel
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Trait for adding "pluggable" loss functions for the gradient boosting algorithm.
 */
@DeveloperApi
trait Loss extends Serializable {

  /**
   * Method to calculate the gradients for the gradient boosting calculation.
   * @param model Model of the weak learner.
   * @param point Instance of the training dataset.
   * @return Loss gradient.
   */
  def gradient(
      model: TreeEnsembleModel,
      point: LabeledPoint): Double

  /**
   * Method to calculate error of the base learner for the gradient boosting calculation.
   * Note: This method is not used by the gradient boosting algorithm but is useful for debugging
   * purposes.
   * @param model Model of the weak learner.
   * @param data Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return Measure of model error on data
   */
  def computeError(model: TreeEnsembleModel, data: RDD[LabeledPoint]): Double

}
