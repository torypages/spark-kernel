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
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Represents the module for creating a SQL Context.
 *
 * @param sparkContext The Spark Context to use to initialize the SQL Context
 */
class SqlContextModule(
  private val sparkContext: SparkContext
) extends ModuleLike with LogLike {
  // TODO: Make this a configuration property
  val AppName = SparkKernelInfo.banner

  private var sqlContext: Option[SQLContext] = None

  override def isInitialized: Boolean = sqlContext.nonEmpty

  override def startImpl(): Unit = {
     sqlContext = Some(try {
      logger.info("Attempting to create Hive Context")
      val hiveContextClassString =
        "org.apache.spark.sql.hive.HiveContext"

      logger.debug(s"Looking up $hiveContextClassString")
      val hiveContextClass = Class.forName(hiveContextClassString)

      val sparkContextClass = classOf[SparkContext]
      val sparkContextClassName = sparkContextClass.getName

      logger.debug(s"Searching for constructor taking $sparkContextClassName")
      val hiveContextContructor =
        hiveContextClass.getConstructor(sparkContextClass)

      logger.debug("Invoking Hive Context constructor")
      hiveContextContructor.newInstance(sparkContext).asInstanceOf[SQLContext]
    } catch {
      case _: Throwable =>
        logger.warn("Unable to create Hive Context! Defaulting to SQL Context!")
        new SQLContext(sparkContext)
    })

    publishArtifact(sqlContext.get)
  }

  override def stopImpl(): Unit = {
    sqlContext = None
  }
}
