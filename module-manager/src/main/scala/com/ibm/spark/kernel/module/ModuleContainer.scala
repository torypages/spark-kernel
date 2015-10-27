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

import org.slf4j.LoggerFactory

/**
 * Represents a container for started modules and their dependencies.
 */
trait ModuleContainer[T <: ModuleLike] {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  import com.softwaremill.macwire.MacwireMacros._

  /** Represents the module manager used to load modules. */
  val moduleManager: ModuleManager[T]

  /** Provides an interface for wiring dependencies together. */
  private val baseWired = wiredInModule(this)
  @volatile private var wired = baseWired

  /**
   * Adds the specified dependencies to the container module.
   *
   * @param moduleArtifacts The artifacts to add as dependencies
   */
  def addDependencies(moduleArtifacts: ModuleArtifact*) =
    wired = wired.withInstances(moduleArtifacts.map(_.value): _*)

  def startAllModules(): Unit = {

  }

  /**
   * Starts the module with the specified class name.
   *
   * @param className The class name of the module to start
   *
   * @return The instantiated and started module
   */
  def startModule(className: String): AnyRef = {
    val moduleInstance = wired.wireClassInstanceByName(className) match {
      case m: ModuleLike => m
      case o =>
        val className = o.getClass.getName
        throw new ModuleException(s"Unknown module type: $className")
    }

    logger.info(s"Starting module: $className")
    moduleInstance.start()

    moduleInstance
  }

  /**
   * Stops the module with the specified class name.
   *
   * @param className The class name of the module to stop
   */
  def stopModule(className: String): Unit = {
    // TODO: Implementation
  }
}
