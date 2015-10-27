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

import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.SparkKernelInfo
import com.ibm.spark.utils.LogLike
import com.typesafe.config.Config
import org.apache.spark.SparkContext

/**
 * Represents the module for adding needed startup jars to the Spark Context.
 *
 * @param config The configuration to use in association with adding jars
 * @param sparkContext The Spark Context to use to add jars to the cluster
 */
class SparkContextJarsModule(
  private val config: Config,
  private val sparkContext: SparkContext
) extends ModuleLike with LogLike {
  // TODO: Make this a configuration property
  val AppName = SparkKernelInfo.banner

  private var _sparkContext: Option[SparkContext] = None

  override def isInitialized: Boolean = _sparkContext.nonEmpty

  override def startImpl(): Unit = {
    _sparkContext = Some(sparkContext)

    // Add ourselves as a dependency
    // TODO: Provide ability to point to library as commandline argument
    // TODO: Provide better method to determine if can add ourselves
    // TODO: Avoid duplicating request for master twice (initializeSparkContext
    //       also does this)
    val master = config.getString("spark.master")
    // If in local mode, do not need to add our jars as dependencies
    if (!master.toLowerCase.startsWith("local")) {
      @inline def getJarPathFor(klass: Class[_]): String =
        klass.getProtectionDomain.getCodeSource.getLocation.getPath

      // TODO: Provide less hard-coded solution in case additional dependencies
      //       are added or classes are refactored to different projects
      val jarPaths = Seq(
        // Macro project
        classOf[com.ibm.spark.annotations.Experimental],

        // Protocol project
        classOf[com.ibm.spark.kernel.protocol.v5.KernelMessage],

        // Communication project
        classOf[com.ibm.spark.communication.SocketManager],

        // Kernel-api project
        classOf[com.ibm.spark.kernel.api.KernelLike],

        // Scala-interpreter project
        classOf[com.ibm.spark.kernel.interpreter.scala.ScalaInterpreter],

        // PySpark-interpreter project
        classOf[com.ibm.spark.kernel.interpreter.pyspark.PySparkInterpreter],

        // SparkR-interpreter project
        classOf[com.ibm.spark.kernel.interpreter.sparkr.SparkRInterpreter],

        // Kernel project
        classOf[com.ibm.spark.boot.KernelBootstrap]
      ).map(getJarPathFor)

      logger.info("Adding kernel jars to cluster:\n- " +
        jarPaths.mkString("\n- "))
      jarPaths.foreach(sparkContext.addJar)
    } else {
      logger.info("Running in local mode! Not adding self as dependency!")
    }
  }

  override def stopImpl(): Unit = {
    _sparkContext = None
  }
}
