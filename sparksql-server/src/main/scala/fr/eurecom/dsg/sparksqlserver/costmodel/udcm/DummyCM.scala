package fr.eurecom.dsg.sparksqlserver.costmodel.udcm

import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel

import scala.util.Random

/**
 * Created by hoang on 6/22/15.
 */
class DummyCM extends CostModel {

  override def compute(): Boolean = {
    Random.nextBoolean()
  }

}
