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
import com.ibm.spark.interpreter.InterpreterContainer
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for registering an interrupt hook.
 */
class InterruptHookModule(
  private val actorSystem: ActorSystem,
  private val interpreterContainer: InterpreterContainer
) extends ModuleLike with LogLike {
  import sun.misc.{Signal, SignalHandler}

  // TODO: Signals are not a good way to handle this since JVM only has the
  // proprietary sun API that is not necessarily available on all platforms
  Signal.handle(new Signal("INT"), new SignalHandler() {
    private val MaxSignalTime: Long = 3000 // 3 seconds
    var lastSignalReceived: Long    = 0

    def handle(sig: Signal) = {
      val currentTime = System.currentTimeMillis()
      if (currentTime - lastSignalReceived > MaxSignalTime) {
        logger.info("Resetting code execution!")
        interpreterContainer.foreach(_.interrupt())

        // TODO: Cancel group representing current code execution
        //sparkContext.cancelJobGroup()

        logger.info("Enter Ctrl-C twice to shutdown!")
        lastSignalReceived = currentTime
      } else {
        logger.info("Shutting down kernel")
        System.exit(0)
      }
    }
  })

  override def isInitialized: Boolean = ???

  override protected def startImpl(): Unit = ???

  override protected def stopImpl(): Unit = ???
}
