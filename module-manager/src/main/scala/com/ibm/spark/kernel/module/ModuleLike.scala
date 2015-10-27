/*
 * Copyright 2015 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.spark.kernel.module

/**
 * Represents a module that can be loaded.
 */
trait ModuleLike {
  @volatile private var currentArtifacts =
    collection.mutable.Seq[ModuleArtifact]()

  /**
   * Returns the collection of module artifacts produced by this module.
   *
   * @return The collection of artifacts
   */
  def getCurrentArtifacts: Array[ModuleArtifact] = currentArtifacts.toArray

  /**
   * Adds the specified artifact to this module's internal collection to
   * be used as a dependency for other modules.
   *
   * @param moduleArtifact The module artifact to add
   */
  def publishArtifact(moduleArtifact: ModuleArtifact): Unit =
    currentArtifacts :+= moduleArtifact

  /**
   * Adds the specified value as an artifact to this module's internal
   * collection to be used as a dependency for other modules.
   *
   * @note Injects the value class name as the name of the artifact
   *
   * @param value The value to publish as an artifact
   */
  def publishArtifact(value: AnyRef): Unit =
    publishArtifact(ModuleArtifact(value, value.getClass.getName))

  /**
   * Adds the specified artifacts to this module's internal collection to
   * be used as dependencies for other modules.
   *
   * @param moduleArtifacts The collection of module artifacts to add
   */
  def publishArtifacts(moduleArtifacts: ModuleArtifact*): Unit =
    moduleArtifacts.foreach(publishArtifact)

  /**
   * Adds the specified values as module artifacts to this module's internal
   * collection to be used as dependencies for other modules.
   *
   * @note Injects the value class names as the names of the artifacts
   *
   * @param values The collection of values to add
   */
  def publishArtifacts(values: AnyRef*): Unit =
    values.foreach(publishArtifact)

  /**
   * Starts the module, initializing any state.
   *
   * @return Collection of new objects created by the module
   */
  final def start(): Unit = {
    assert(!isInitialized, "Module already initialized!")

    startImpl()
  }

  /**
   * The implementation of start to be provided by implementors of modules.
   */
  protected def startImpl(): Unit

  /** Stops the module, clearing any state. */
  final def stop(): Unit = {
    assert(isInitialized, "Module not initialized!")

    stopImpl()
  }

  /**
   * The implementation of stop to be provided by implementors of modules.
   */
  protected def stopImpl(): Unit

  /**
   * Indicates whether or not the module is initialized.
   *
   * @return True if initialized, otherwise false
   */
  def isInitialized: Boolean

  /**
   * Indicates whether or not the module's preconditions have been met such
   * that it can be started.
   *
   * @return True if the conditions have been met, otherwise false
   */
  def preconditionsMet: Boolean = true
}
