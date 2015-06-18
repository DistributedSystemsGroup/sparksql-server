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

package org.apache.spark.scheduler

import scala.collection.mutable

import akka.actor.{ActorRef, Actor}

import org.apache.spark._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(stage: Int, task: Long, taskAttempt: Long)

/**
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorActor, so requests to commit
 * output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf) extends Logging {

  // Initialized by SparkEnv
  var coordinatorActor: Option[ActorRef] = None
  private val timeout = AkkaUtils.askTimeout(conf)
  private val maxAttempts = AkkaUtils.numRetries(conf)
  private val retryInterval = AkkaUtils.retryWaitMs(conf)

  private type StageId = Int
  private type PartitionId = Long
  private type TaskAttemptId = Long

  /**
   * Map from active stages's id => partition id => task attempt with exclusive lock on committing
   * output for that partition.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val authorizedCommittersByStage: CommittersByStageMap = mutable.Map()
  private type CommittersByStageMap = mutable.Map[StageId, mutable.Map[PartitionId, TaskAttemptId]]

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    authorizedCommittersByStage.isEmpty
  }

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attempt a unique identifier for this task attempt
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: StageId,
      partition: PartitionId,
      attempt: TaskAttemptId): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, partition, attempt)
    coordinatorActor match {
      case Some(actor) =>
        AkkaUtils.askWithReply[Boolean](msg, actor, maxAttempts, retryInterval, timeout)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  // Called by DAGScheduler
  private[scheduler] def stageStart(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage(stage) = mutable.HashMap[PartitionId, TaskAttemptId]()
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: StageId,
      partition: PartitionId,
      attempt: TaskAttemptId,
      reason: TaskEndReason): Unit = synchronized {
    val authorizedCommitters = authorizedCommittersByStage.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case denied: TaskCommitDenied =>
        logInfo(
          s"Task was denied committing, stage: $stage, partition: $partition, attempt: $attempt")
      case otherReason =>
        if (authorizedCommitters.get(partition).exists(_ == attempt)) {
          logDebug(s"Authorized committer $attempt (stage=$stage, partition=$partition) failed;" +
            s" clearing lock")
          authorizedCommitters.remove(partition)
        }
    }
  }

  def stop(): Unit = synchronized {
    coordinatorActor.foreach(_ ! StopCoordinator)
    coordinatorActor = None
    authorizedCommittersByStage.clear()
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
      stage: StageId,
      partition: PartitionId,
      attempt: TaskAttemptId): Boolean = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        authorizedCommitters.get(partition) match {
          case Some(existingCommitter) =>
            logDebug(s"Denying $attempt to commit for stage=$stage, partition=$partition; " +
              s"existingCommitter = $existingCommitter")
            false
          case None =>
            logDebug(s"Authorizing $attempt to commit for stage=$stage, partition=$partition")
            authorizedCommitters(partition) = attempt
            true
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing task attempt $attempt to commit")
        false
    }
  }
}

private[spark] object OutputCommitCoordinator {

  // This actor is used only for RPC
  class OutputCommitCoordinatorActor(outputCommitCoordinator: OutputCommitCoordinator)
    extends Actor with ActorLogReceive with Logging {

    override def receiveWithLogging = {
      case AskPermissionToCommitOutput(stage, partition, taskAttempt) =>
        sender ! outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, taskAttempt)
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        context.stop(self)
        sender ! true
    }
  }
}
