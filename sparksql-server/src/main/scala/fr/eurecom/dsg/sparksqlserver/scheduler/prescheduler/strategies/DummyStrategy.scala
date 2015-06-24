package fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler.strategies

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer
import fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler.PreSchedulingStrategy

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/24/15.
 */
class DummyStrategy extends PreSchedulingStrategy {
  override def execute(dagLst : ArrayBuffer[DAGContainer]): ArrayBuffer[DAGContainer] = {
    dagLst
  }
}
