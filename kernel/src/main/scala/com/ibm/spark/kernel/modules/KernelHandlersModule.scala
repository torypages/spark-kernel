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
import com.ibm.spark.comm.{CommStorage, CommRegistrar}
import com.ibm.spark.kernel.api.KernelLike
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.{SocketType, MessageType}
import com.ibm.spark.kernel.protocol.v5.MessageType.MessageType
import com.ibm.spark.kernel.protocol.v5.SocketType.SocketType
import com.ibm.spark.kernel.protocol.v5.handler._
import com.ibm.spark.kernel.protocol.v5.kernel.ActorLoader
import com.ibm.spark.utils.{ResponseMap, LogLike}

/**
 * Represents a module for creating kernel handler actors.
 *
 * @param actorSystem The actor system to use to create the handlers
 * @param actorLoader The actor loader to provide to some handlers
 * @param responseMap The response map to provide to some handlers
 * @param kernelApi The kernel api to provide to some handlers
 * @param commRegistrar The Comm registrar to provide to some handlers
 * @param commStorage The Comm storage to provide to some handlers
 */
class KernelHandlersModule(
  private val actorSystem: ActorSystem,
  private val actorLoader: ActorLoader,
  private val responseMap: ResponseMap,
  private val kernelApi: KernelLike,
  private val commRegistrar: CommRegistrar,
  private val commStorage: CommStorage
) extends ModuleLike with LogLike {
  private var actors: Option[Seq[ActorRef]] = None

  override def isInitialized: Boolean = actors.nonEmpty

  override def startImpl(): Unit = {
    var _actors = collection.mutable.Seq[ActorRef]()

    //  These are the handlers for messages coming into the
    _actors +:= initializeRequestHandler(classOf[ExecuteRequestHandler],
      MessageType.Incoming.ExecuteRequest, kernelApi)
    _actors +:= initializeRequestHandler(classOf[KernelInfoRequestHandler],
      MessageType.Incoming.KernelInfoRequest)
    _actors +:= initializeRequestHandler(classOf[CodeCompleteHandler],
      MessageType.Incoming.CompleteRequest)
    _actors +:= initializeInputHandler(classOf[InputRequestReplyHandler],
      MessageType.Incoming.InputReply)
    _actors +:= initializeCommHandler(classOf[CommOpenHandler],
      MessageType.Incoming.CommOpen)
    _actors +:= initializeCommHandler(classOf[CommMsgHandler],
      MessageType.Incoming.CommMsg)
    _actors +:= initializeCommHandler(classOf[CommCloseHandler],
      MessageType.Incoming.CommClose)

    //  These are handlers for messages leaving the kernel through the sockets
    _actors +:= initializeSocketHandler(SocketType.Shell, MessageType.Outgoing.KernelInfoReply)
    _actors +:= initializeSocketHandler(SocketType.Shell, MessageType.Outgoing.ExecuteReply)
    _actors +:= initializeSocketHandler(SocketType.Shell, MessageType.Outgoing.CompleteReply)

    _actors +:= initializeSocketHandler(SocketType.StdIn, MessageType.Outgoing.InputRequest)

    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.ExecuteResult)
    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.Stream)
    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.ExecuteInput)
    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.Status)
    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.Error)
    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.CommOpen)
    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.CommMsg)
    _actors +:= initializeSocketHandler(SocketType.IOPub, MessageType.Outgoing.CommClose)

    actors = Some(_actors)
  }

  override def stopImpl(): Unit = {
    actors.get.foreach(_ ! PoisonPill)
    actors = None
  }


  private def initializeRequestHandler[T](clazz: Class[T], messageType: MessageType, extraArguments: AnyRef*): ActorRef = {
    logger.debug("Creating %s handler".format(messageType.toString))
    actorSystem.actorOf(
      Props(clazz, actorLoader +: extraArguments: _*),
      name = messageType.toString
    )
  }

  private def initializeInputHandler[T](
    clazz: Class[T],
    messageType: MessageType
  ): ActorRef = {
    logger.debug("Creating %s handler".format(messageType.toString))
    actorSystem.actorOf(
      Props(clazz, actorLoader, responseMap),
      name = messageType.toString
    )
  }

  // TODO: Figure out how to pass variable number of arguments to actor
  private def initializeCommHandler[T](clazz: Class[T], messageType: MessageType): ActorRef = {
    logger.debug("Creating %s handler".format(messageType.toString))
    actorSystem.actorOf(
      Props(clazz, actorLoader, commRegistrar, commStorage),
      name = messageType.toString
    )
  }

  private def initializeSocketHandler(socketType: SocketType, messageType: MessageType): ActorRef = {
    logger.debug("Creating %s to %s socket handler ".format(messageType.toString ,socketType.toString))
    actorSystem.actorOf(
      Props(classOf[GenericSocketMessageHandler], actorLoader, socketType),
      name = messageType.toString
    )
  }
}
