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
import com.ibm.spark.kernel.protocol.v5.kernel.{SimpleActorLoader, ActorLoader}
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for creating an actor system.
 *
 * @param actorSystem The actor system to use with the actor loader
 */
class ActorLoaderModule(private val actorSystem: ActorSystem)
  extends ModuleLike with LogLike
{
  private var actorLoader: Option[ActorLoader] = None

  override def isInitialized: Boolean = actorLoader.nonEmpty

  override def startImpl(): Unit = {
    logger.info(s"Initializing actor loader")
    actorLoader = Some(new SimpleActorLoader(actorSystem))

    publishArtifact(actorLoader.get)
  }

  override def stopImpl(): Unit = {
    actorLoader = None
  }
}
