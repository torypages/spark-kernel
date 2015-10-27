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

import com.ibm.spark.kernel.api.ReplClassServerInfo
import com.ibm.spark.kernel.interpreter.scala._
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.utils.{TaskManager, LogLike}
import com.typesafe.config.Config
import scala.collection.JavaConverters._

/**
 * Represents the module for the Scala interpreter.
 *
 * @param config The configuration to use when creating the interpreter
 */
class ScalaInterpreterModule(
  private val config: Config
) extends ModuleLike with LogLike {
  private var scalaInterpreter: Option[ScalaInterpreter] = None

  override def isInitialized: Boolean = scalaInterpreter.nonEmpty

  override def startImpl(): Unit = {
    val interpreterArgs =
      config.getStringList("interpreter_args").asScala.toList
    val maxInterpreterThreads = config.getInt("max_interpreter_threads")

    logger.info(Seq(
      s"Constructing interpreter with $maxInterpreterThreads threads and",
      s"with arguments: '${interpreterArgs.mkString(" ")}'"
    ).mkString(" "))
    scalaInterpreter = Some(new ScalaInterpreter(interpreterArgs, Console.out)
      with StandardSparkIMainProducer
      with TaskManagerProducerLike
      with StandardSettingsProducer {
      override def newTaskManager(): TaskManager =
        new TaskManager(maximumWorkers = maxInterpreterThreads)
    })

    logger.debug("Starting interpreter")
    scalaInterpreter.get.start()

    publishArtifacts(
      scalaInterpreter.get,
      ReplClassServerInfo(uriString = scalaInterpreter.get.classServerURI)
    )
  }

  override def stopImpl(): Unit = {
    assert(!isInitialized, "Module not initialized!")

    scalaInterpreter.get.stop()
    scalaInterpreter = None
  }
}
