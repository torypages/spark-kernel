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

import com.ibm.spark.global.StreamState
import com.ibm.spark.kernel.api.ReplClassServerInfo
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.stream.KernelOutputStream
import com.ibm.spark.kernel.protocol.v5.{SparkKernelInfo, KMBuilder}
import com.ibm.spark.kernel.protocol.v5.kernel.ActorLoader
import com.ibm.spark.utils.{ScheduledTaskManager, KeyValuePairUtils, LogLike}
import com.typesafe.config.Config
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try

/**
 * Represents the module for creating a Spark Context.
 *
 * @param config The configuration to use when creating the context
 * @param actorLoader The actor loader to use
 * @param streamState The state to hold input/output streams
 * @param scheduledTaskManager The task manager to use for the kernel output
 *                             stream being registered with the Spark Context
 * @param kernelMessageBuilder The kernel message builder to use to create the
 *                             messages for the kernel output stream
 * @param replClassServerInfo The container of repl class server information
 *                            used to initialize the Spark Context
 */
class SparkContextModule(
  private val config: Config,
  private val actorLoader: ActorLoader,
  private val streamState: StreamState,
  private val scheduledTaskManager: ScheduledTaskManager,
  private val kernelMessageBuilder: KMBuilder,
  private val replClassServerInfo: ReplClassServerInfo
) extends ModuleLike with LogLike {
  // TODO: Make this a configuration property
  val AppName = SparkKernelInfo.banner

  private var sparkContext: Option[SparkContext] = None

  override def isInitialized: Boolean = sparkContext.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Creating Spark Configuration")
    val sparkConf = new SparkConf()

    val master = config.getString("spark.master")
    logger.info(s"Using $master as Spark Master")
    sparkConf.setMaster(master)

    logger.info(s"Using $AppName as Spark application name")
    sparkConf.setAppName(AppName)

    KeyValuePairUtils.stringToKeyValuePairSeq(
      config.getString("spark_configuration")
    ).foreach { keyValuePair =>
      logger.info(s"Setting ${keyValuePair.key} to ${keyValuePair.value}")
      Try(sparkConf.set(keyValuePair.key, keyValuePair.value))
    }

    logger.info("REPL Class Server Uri: " + replClassServerInfo.uriString)
    sparkConf.set("spark.repl.class.uri", replClassServerInfo.uriString)

    // TODO: Inject stream redirect headers in Spark dynamically
    val outStream = new KernelOutputStream(
      actorLoader, kernelMessageBuilder, scheduledTaskManager,
      sendEmptyOutput = config.getBoolean("send_empty_output")
    )

    // Update global stream state and use it to set the Console local variables
    // for threads in the Spark threadpool
    logger.debug("Setting initial stream state")
    streamState.setStreams(System.in, outStream, outStream)

    logger.debug("Constructing new Spark Context")
    streamState.withStreams {
      sparkContext = Some(new SparkContext(sparkConf))
    }

    publishArtifact(sparkContext.get)
  }

  override def stopImpl(): Unit = {
    sparkContext.get.stop()
    sparkContext = None
  }
}
