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
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.utils.LogLike
import com.typesafe.config.Config

/**
 * Represents the module for creating an actor system.
 *
 * @param config The configuration to use to lookup the actor system name
 */
class ActorSystemModule(
  private val config: Config
) extends ModuleLike with LogLike {
  // TODO: Move to retrieval of name from config
  private val ActorSystemName = "IBM Spark Kernel"
  private var actorSystem: Option[ActorSystem] = None

  override def isInitialized: Boolean = actorSystem.nonEmpty

  override def startImpl(): Unit = {
    logger.info(s"Initializing internal actor system '$ActorSystemName'")
    actorSystem = Some(ActorSystem(ActorSystemName))

    publishArtifact(actorSystem.get)
  }

  override def stopImpl(): Unit = {
    actorSystem.get.shutdown()
    actorSystem = None
  }
}
