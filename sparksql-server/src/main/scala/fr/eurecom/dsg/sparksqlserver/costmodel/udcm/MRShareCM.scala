package fr.eurecom.dsg.sparksqlserver.costmodel.udcm

import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel

/**
 * Created by hoang on 6/17/15.
 */
class MRShareCM extends CostModel {

  private var f : Double = 0

  override def compute() : Unit = {

  }

  def GAIN(): Double = {
    0
  }

  def GS(): Double = {
    0
  }



}
