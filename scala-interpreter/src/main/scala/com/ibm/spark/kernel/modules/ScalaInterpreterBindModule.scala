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

import com.ibm.spark.kernel.api.KernelLike
import com.ibm.spark.kernel.interpreter.scala._
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.utils.LogLike
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Represents the module for binding data to an existing Scala Interpreter.
 *
 * @param scalaInterpreter The interpreter to bind data into
 * @param sparkContext The Spark Context to bind into the interpreter
 * @param sqlContext The SQL Context to bind into the interpreter
 * @param kernelApi The kernel API to bind into the interpreter
 */
class ScalaInterpreterBindModule(
  private val scalaInterpreter: ScalaInterpreter,
  private val sparkContext: SparkContext,
  private val sqlContext: SQLContext,
  private val kernelApi: KernelLike
) extends ModuleLike with LogLike {
  private var _scalaInterpreter: Option[ScalaInterpreter] = None

  override def isInitialized: Boolean = _scalaInterpreter.nonEmpty

  override def startImpl(): Unit = {
    assert(isInitialized, "Module already initialized!")

    _scalaInterpreter = Some(scalaInterpreter)

    addSparkContext(sparkContext)
    initializeSparkCluster()
    addSqlContext(sqlContext)
    addKernelApi(kernelApi)
  }

  override def stopImpl(): Unit = {
    assert(!isInitialized, "Module not initialized!")

    _scalaInterpreter = None
  }

  private def addSparkContext(sparkContext: SparkContext): Unit = {
    val scalaInterpreter = _scalaInterpreter.get

    scalaInterpreter.doQuietly {
      logger.debug("Binding Spark Context into Scala interpreter")
      scalaInterpreter.bind(
        "sc",
        classOf[SparkContext].getName,
        sparkContext,
        List( """@transient""")
      )

    }
  }

  private def addSqlContext(sqlContext: SQLContext): Unit = {
    val scalaInterpreter = _scalaInterpreter.get

    scalaInterpreter.doQuietly {
      logger.debug("Binding SQL Context into Scala interpreter")
      scalaInterpreter.bind(
        "sqlContext",
        classOf[SQLContext].getName,
        sqlContext,
        List("""@transient""")
      )
    }
  }

  private def addKernelApi(kernelApi: KernelLike): Unit = {
    val scalaInterpreter = _scalaInterpreter.get

    scalaInterpreter.doQuietly {
      logger.debug("Binding kernel API into Scala interpreter")
      scalaInterpreter.bind(
        "kernel", classOf[KernelLike].getName,
        kernelApi, List( """@transient implicit""")
      )
    }
  }

  private def initializeSparkCluster(): Unit = {
    val scalaInterpreter = _scalaInterpreter.get

    // NOTE: This is needed because interpreter blows up after adding
    //       dependencies to SparkContext and Interpreter before the
    //       cluster has been used... not exactly sure why this is the case
    // TODO: Investigate why the cluster has to be initialized in the kernel
    //       to avoid the kernel's interpreter blowing up (must be done
    //       inside the interpreter)
    scalaInterpreter.doQuietly {
      logger.debug("Initializing Spark cluster in Scala interpreter")
      scalaInterpreter.interpret("""
        |val $toBeNulled = {
        |  var $toBeNulled = sc.emptyRDD.collect()
        |  $toBeNulled = null
        |}
      """.stripMargin)
    }
  }
}
