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

import akka.actor.ActorSystem
import com.ibm.spark.comm.{KernelCommManager, CommManager, CommRegistrar, CommStorage}
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.KMBuilder
import com.ibm.spark.kernel.protocol.v5.kernel.{ActorLoader, SimpleActorLoader}
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for creating Comm components.
 *
 * @param actorSystem The actor system to use
 * @param actorLoader The actor loader to use
 * @param kernelMessageBuilder The kernel message builder to use
 */
class CommModule(
  private val actorSystem: ActorSystem,
  private val actorLoader: ActorLoader,
  private val kernelMessageBuilder: KMBuilder
) extends ModuleLike with LogLike {
  private var commStorage: Option[CommStorage] = None
  private var commRegistrar: Option[CommRegistrar] = None
  private var commManager: Option[CommManager] = None

  override def isInitialized: Boolean =
    commStorage.nonEmpty &&
    commRegistrar.nonEmpty &&
    commManager.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Constructing Comm storage")
    commStorage = Some(new CommStorage())

    logger.debug("Constructing Comm registrar")
    commRegistrar = Some(new CommRegistrar(commStorage.get))

    logger.debug("Constructing Comm manager")
    commManager = Some(new KernelCommManager(
      actorLoader, KMBuilder(), commRegistrar.get
    ))

    publishArtifacts(commStorage.get, commRegistrar.get, commManager.get)
  }

  override def stopImpl(): Unit = {
    commManager = None
    commRegistrar = None
    commStorage = None
  }
}
