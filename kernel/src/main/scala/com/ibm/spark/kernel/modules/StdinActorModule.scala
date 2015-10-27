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
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.SocketType
import com.ibm.spark.kernel.protocol.v5.kernel.ActorLoader
import com.ibm.spark.kernel.protocol.v5.kernel.socket.{Stdin, Heartbeat, SocketConfig, SocketFactory}
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for creating a standard in actor.
 *
 * @param actorSystem The actor system to use
 * @param actorLoader The actor loader to use
 * @param socketConfig The socket configuration to use
 * @param socketFactory The factory to create sockets
 */
class StdinActorModule(
  private val actorSystem: ActorSystem,
  private val actorLoader: ActorLoader,
  private val socketConfig: SocketConfig,
  private val socketFactory: SocketFactory
) extends ModuleLike with LogLike {
  private var stdinActor: Option[ActorRef] = None

  override def isInitialized: Boolean = stdinActor.nonEmpty

  override def startImpl(): Unit = {
    val port = socketConfig.stdin_port

    logger.debug(s"Initializing Stdin on port $port")
    stdinActor = Some(actorSystem.actorOf(
      Props(classOf[Stdin], socketFactory, actorLoader),
      name = SocketType.StdIn.toString
    ))
  }

  override def stopImpl(): Unit = {
    stdinActor.get ! PoisonPill
    stdinActor = None
  }
}
