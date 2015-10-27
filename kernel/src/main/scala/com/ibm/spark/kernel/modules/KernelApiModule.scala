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

import com.ibm.spark.comm.CommManager
import com.ibm.spark.interpreter.DefaultInterpreter
import com.ibm.spark.kernel.api.Kernel
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.kernel.ActorLoader
import com.ibm.spark.magic.MagicLoader
import com.ibm.spark.utils.LogLike
import com.typesafe.config.Config

/**
 * Represents the module for creating the kernel api instance.
 */
class KernelApiModule(
  private val config: Config,
  private val actorLoader: ActorLoader,
  private val defaultInterpreter: DefaultInterpreter,
  private val commManager: CommManager,
  private val magicLoader: MagicLoader
) extends ModuleLike with LogLike {
  private var kernelApi: Option[Kernel] = None

  override def isInitialized: Boolean = kernelApi.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Creating kernel api")
    kernelApi = Some(new Kernel(
      config,
      actorLoader,
      defaultInterpreter,
      commManager,
      magicLoader
    ))

    publishArtifact(kernelApi.get)
  }

  override def stopImpl(): Unit = {
    kernelApi = None
  }
}
