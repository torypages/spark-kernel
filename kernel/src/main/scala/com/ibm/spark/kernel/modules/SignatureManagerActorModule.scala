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

import akka.actor.{PoisonPill, ActorRef, ActorSystem, Props}
import com.ibm.spark.communication.security.{SecurityActorType, SignatureManagerActor}
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.utils.LogLike
import com.typesafe.config.Config

/**
 * Represents the module for creating a signature manager actor.
 *
 * @param config The configuration to use
 * @param actorSystem The actor system to use
 */
class SignatureManagerActorModule(
  private val config: Config,
  private val actorSystem: ActorSystem
) extends ModuleLike with LogLike {
  private var signatureManagerActor: Option[ActorRef] = None

  override def isInitialized: Boolean = signatureManagerActor.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Creating signature manager actor")
    val sigKey = config.getString("key")
    val sigScheme = config.getString("signature_scheme")
    logger.debug("Key = " + sigKey)
    logger.debug("Scheme = " + sigScheme)
    signatureManagerActor = Some(actorSystem.actorOf(
      Props(
        classOf[SignatureManagerActor], sigKey, sigScheme.replace("-", "")
      ),
      name = SecurityActorType.SignatureManager.toString
    ))
  }

  override def stopImpl(): Unit = {
    signatureManagerActor.get ! PoisonPill
    signatureManagerActor = None
  }
}
