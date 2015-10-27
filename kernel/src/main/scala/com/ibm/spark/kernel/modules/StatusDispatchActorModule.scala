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

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import com.ibm.spark.communication.security.{SecurityActorType, SignatureManagerActor}
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.SystemActorType
import com.ibm.spark.kernel.protocol.v5.dispatch.StatusDispatch
import com.ibm.spark.kernel.protocol.v5.kernel.ActorLoader
import com.ibm.spark.utils.LogLike
import com.typesafe.config.Config

/**
 * Represents the module for creating a status dispatch actor.
 *
 * @param actorSystem The actor system to use
 * @param actorLoader The actor loader to use
 */
class StatusDispatchActorModule(
  private val actorSystem: ActorSystem,
  private val actorLoader: ActorLoader
) extends ModuleLike with LogLike {
  private var statusDispatchActor: Option[ActorRef] = None

  override def isInitialized: Boolean = statusDispatchActor.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Creating status dispatch actor")
    statusDispatchActor = Some(actorSystem.actorOf(
      Props(classOf[StatusDispatch], actorLoader),
      name = SystemActorType.StatusDispatch.toString
    ))
  }

  override def stopImpl(): Unit = {
    statusDispatchActor.get ! PoisonPill
    statusDispatchActor = None
  }
}
