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

import com.ibm.spark.dependencies.DependencyDownloader
import com.ibm.spark.interpreter.DefaultInterpreter
import com.ibm.spark.kernel.api.KernelLike
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.magic.MagicLoader
import com.ibm.spark.magic.builtin.BuiltinLoader
import com.ibm.spark.magic.dependencies.DependencyMap
import com.ibm.spark.utils.{MultiClassLoader, LogLike}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

/**
 * Represents the module for creating the kernel api instance.
 *
 * @param config The configuration to use
 * @param dependencyMap The dependency map to provide to the magic loader
 * @param defaultInterpreter The interpreter to serve as the default for
 *                           injection into magics
 * @param dependencyDownloader The utility used to download dependencies
 * @param kernelApi The kernel API instance to use for injection into magics
 * @param sparkContext The Spark Context to use for injection into magics
 */
class MagicLoaderModule(
  private val config: Config,
  private val dependencyMap: DependencyMap,
  private val defaultInterpreter: DefaultInterpreter,
  private val dependencyDownloader: DependencyDownloader,
  private val kernelApi: KernelLike,
  private val sparkContext: SparkContext
) extends ModuleLike with LogLike {
  private var magicLoader: Option[MagicLoader] = None

  override def isInitialized: Boolean = magicLoader.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Constructing magic loader")

    logger.debug("Building dependency map")
    dependencyMap
      .setInterpreter(defaultInterpreter)
      .setKernelInterpreter(defaultInterpreter) // This is deprecated
      .setSparkContext(sparkContext)
      .setDependencyDownloader(dependencyDownloader)
      .setKernel(kernelApi)

    logger.debug("Creating BuiltinLoader")
    val builtinLoader = new BuiltinLoader()

    val magicUrlArray = config.getStringList("magic_urls").asScala
      .map(s => new java.net.URL(s)).toArray

    if (magicUrlArray.isEmpty)
      logger.warn("No external magics provided to MagicLoader!")
    else
      logger.info("Using magics from the following locations: " +
        magicUrlArray.map(_.getPath).mkString(","))

    val multiClassLoader = new MultiClassLoader(
      builtinLoader,
      defaultInterpreter.classLoader
    )

    logger.debug("Creating MagicLoader")
    magicLoader = Some(new MagicLoader(
      dependencyMap = dependencyMap,
      urls = magicUrlArray,
      parentLoader = multiClassLoader
    ))
    magicLoader.get.dependencyMap.setMagicLoader(magicLoader.get)

    publishArtifact(magicLoader.get)
  }

  override def stopImpl(): Unit = {
    magicLoader = None
  }
}
