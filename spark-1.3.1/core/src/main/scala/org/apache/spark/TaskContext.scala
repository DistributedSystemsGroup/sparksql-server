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

package org.apache.spark

import java.io.Serializable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.TaskCompletionListener


object TaskContext {
  /**
   * Return the currently active TaskContext. This can be called inside of
   * user functions to access contextual information about running tasks.
   */
  def get(): TaskContext = taskContext.get

  private val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  // Note: protected[spark] instead of private[spark] to prevent the following two from
  // showing up in JavaDoc.
  /**
   * Set the thread local TaskContext. Internal to Spark.
   */
  protected[spark] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  /**
   * Unset the thread local TaskContext. Internal to Spark.
   */
  protected[spark] def unset(): Unit = taskContext.remove()
}


/**
 * Contextual information about a task which can be read or mutated during
 * execution. To access the TaskContext for a running task, use:
 * {{{
 *   org.apache.spark.TaskContext.get()
 * }}}
 */
abstract class TaskContext extends Serializable {
  // Note: TaskContext must NOT define a get method. Otherwise it will prevent the Scala compiler
  // from generating a static get method (based on the companion object's get method).

  // Note: Update JavaTaskContextCompileCheck when new methods are added to this class.

  // Note: getters in this class are defined with parentheses to maintain backward compatibility.

  /**
   * Returns true if the task has completed.
   */
  def isCompleted(): Boolean

  /**
   * Returns true if the task has been killed.
   */
  def isInterrupted(): Boolean

  @deprecated("use isRunningLocally", "1.2.0")
  def runningLocally(): Boolean

  /**
   * Returns true if the task is running locally in the driver program.
   * @return
   */
  def isRunningLocally(): Boolean

  /**
   * Adds a (Java friendly) listener to be executed on task completion.
   * This will be called in all situation - success, failure, or cancellation.
   * An example use is for HadoopRDD to register a callback to close the input stream.
   */
  def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

  /**
   * Adds a listener in the form of a Scala closure to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation.
   * An example use is for HadoopRDD to register a callback to close the input stream.
   */
  def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext

  /**
   * Adds a callback function to be executed on task completion. An example use
   * is for HadoopRDD to register a callback to close the input stream.
   * Will be called in any situation - success, failure, or cancellation.
   *
   * @param f Callback function.
   */
  @deprecated("use addTaskCompletionListener", "1.2.0")
  def addOnCompleteCallback(f: () => Unit)

  /**
   * The ID of the stage that this task belong to.
   */
  def stageId(): Int

  /**
   * The ID of the RDD partition that is computed by this task.
   */
  def partitionId(): Int

  /**
   * How many times this task has been attempted.  The first task attempt will be assigned
   * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
   */
  def attemptNumber(): Int

  @deprecated("use attemptNumber", "1.3.0")
  def attemptId(): Long

  /**
   * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
   * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
   */
  def taskAttemptId(): Long

  /** ::DeveloperApi:: */
  @DeveloperApi
  def taskMetrics(): TaskMetrics
}
