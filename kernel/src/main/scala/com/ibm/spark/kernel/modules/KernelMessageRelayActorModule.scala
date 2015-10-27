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

import akka.actor.{PoisonPill, ActorRef, Props, ActorSystem}
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.{SystemActorType, MessageType}
import com.ibm.spark.kernel.protocol.v5.content.{CommClose, CommMsg, CommOpen}
import com.ibm.spark.kernel.protocol.v5.relay.KernelMessageRelay
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for creating a kernel message relay actor.
 *
 * @param actorSystem The actor system to use
 */
class KernelMessageRelayActorModule(private val actorSystem: ActorSystem)
  extends ModuleLike with LogLike
{
  private var kernelMessageRelayActor: Option[ActorRef] = None

  override def isInitialized: Boolean = kernelMessageRelayActor.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Creating kernel message relay actor")
    kernelMessageRelayActor = Some(actorSystem.actorOf(
      Props(
        classOf[KernelMessageRelay], kernelMessageRelayActor, true,
        Map(
          CommOpen.toTypeString -> MessageType.Incoming.CommOpen.toString,
          CommMsg.toTypeString -> MessageType.Incoming.CommMsg.toString,
          CommClose.toTypeString -> MessageType.Incoming.CommClose.toString
        ),
        Map(
          CommOpen.toTypeString -> MessageType.Outgoing.CommOpen.toString,
          CommMsg.toTypeString -> MessageType.Outgoing.CommMsg.toString,
          CommClose.toTypeString -> MessageType.Outgoing.CommClose.toString
        )
      ),
      name = SystemActorType.KernelMessageRelay.toString
    ))
  }

  override def stopImpl(): Unit = {
    kernelMessageRelayActor.get ! PoisonPill
    kernelMessageRelayActor = None
  }
}
