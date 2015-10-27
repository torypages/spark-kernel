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

package com.ibm.spark.kernel.modules

import com.ibm.spark.interpreter.DefaultInterpreter
import com.ibm.spark.kernel.module.{ModuleArtifactContainer, ModuleLike}
import com.ibm.spark.utils.LogLike
import com.typesafe.config.Config

/**
 * Represents the module used to mark the default interpreter.
 *
 * @param config The configuration used to specify the default interpreter
 * @param moduleArtifactContainer The container of all module artifacts that
 *                                have been published
 */
class DefaultInterpreterModule(
  private val config: Config,
  private val moduleArtifactContainer: ModuleArtifactContainer
) extends ModuleLike with LogLike {
  @volatile private var defaultInterpreter: Option[DefaultInterpreter] = None

  private val defaultInterpreterString = config.getString("default_interpreter")

  // TODO: Provide better
  private val defaultInterpreterClassString =
    defaultInterpreterString.toLowerCase match {
      case "scala" =>
        logger.info("Using Scala interpreter as default!")
        config.getString("scala_interpreter_class")
      case "pyspark" =>
        logger.info("Using PySpark interpreter as default!")
        config.getString("pyspark_interpreter_class")
      case "sparkr" =>
        logger.info("Using SparkR interpreter as default!")
        config.getString("sparkr_interpreter_class")
      case "sql" =>
        logger.info("Using SQL interpreter as default!")
        config.getString("sql_interpreter_class")
      case _ =>
        val unknown = defaultInterpreterString
        logger.warn(s"Defaulting to unknown interpreter '$unknown'!")
        unknown
    }

  private val interpreterClass = Class.forName(
    defaultInterpreterClassString,
    true,
    this.getClass.getClassLoader
  )

  override def isInitialized: Boolean =
    defaultInterpreter.nonEmpty

  /**
   * Precondition is that an interpreter has been initialized that matches
   * the default interpreter type.
   *
   * @return True if the interpreter is found, otherwise false
   */
  override def preconditionsMet: Boolean = moduleArtifactContainer.exists(a => {
    a.`class` == interpreterClass
  })

  override protected def startImpl(): Unit = {
    defaultInterpreter = moduleArtifactContainer
      .find(_.`class` == interpreterClass)
      .map(_.value)
      .map(_.asInstanceOf[DefaultInterpreter])

    publishArtifact(defaultInterpreter.get)
  }

  override protected def stopImpl(): Unit =
    defaultInterpreter = None
}
