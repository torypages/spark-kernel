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

import akka.actor.{PoisonPill, Props, ActorSystem, ActorRef}
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.SystemActorType
import com.ibm.spark.kernel.protocol.v5.interpreter.InterpreterActor
import com.ibm.spark.kernel.protocol.v5.interpreter.tasks.InterpreterTaskFactory
import com.ibm.spark.kernel.protocol.v5.kernel.ActorLoader
import com.ibm.spark.kernel.protocol.v5.magic.{MagicParser, PostProcessor}
import com.ibm.spark.kernel.protocol.v5.relay.ExecuteRequestRelay
import com.ibm.spark.magic.MagicLoader
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for creating system actors.
 *
 * @param actorSystem The actor system used to create the system actors
 * @param actorLoader The actor loader to provide to the system actors
 * @param magicLoader The magic loader to provide to the system actors
 * @param postProcessor The post processor to use with the execute request
 *                      relay actor
 * @param magicParser The magic parser to use with the execute request
 *                    relay actor
 * @param interpreterTaskFactory The interpreter task factory to use with the
 *                               interpreter actor
 */
class SystemActorsModule(
  private val actorSystem: ActorSystem,
  private val actorLoader: ActorLoader,
  private val magicLoader: MagicLoader,
  private val postProcessor: PostProcessor,
  private val magicParser: MagicParser,
  private val interpreterTaskFactory: InterpreterTaskFactory
) extends ModuleLike with LogLike {
  private var interpreterActor: Option[ActorRef] = None
  private var executeRequestRelayActor: Option[ActorRef] = None

  override def isInitialized: Boolean =
    interpreterActor.nonEmpty &&
    executeRequestRelayActor.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Creating interpreter actor")
    interpreterActor = Some(actorSystem.actorOf(
      Props(classOf[InterpreterActor], interpreterTaskFactory),
      name = SystemActorType.Interpreter.toString
    ))

    logger.debug("Creating execute request relay actor")
    executeRequestRelayActor = Some(actorSystem.actorOf(
      Props(classOf[ExecuteRequestRelay],
        actorLoader, magicLoader, magicParser, postProcessor
      ),
      name = SystemActorType.ExecuteRequestRelay.toString
    ))
  }

  override def stopImpl(): Unit = {
    executeRequestRelayActor.get ! PoisonPill
    executeRequestRelayActor = None

    interpreterActor.get ! PoisonPill
    interpreterActor = None
  }
}
