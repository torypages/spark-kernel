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

import com.ibm.spark.interpreter.DefaultInterpreter
import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.kernel.protocol.v5.interpreter.tasks.InterpreterTaskFactory
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for creating interpreter task factories.
 *
 * @param defaultInterpreter The interpreter whose task factories to create
 */
class InterpreterTaskFactoryModule(
  private val defaultInterpreter: DefaultInterpreter
) extends ModuleLike with LogLike {
  private var interpreterTaskFactory: Option[InterpreterTaskFactory] = None

  override def isInitialized: Boolean = interpreterTaskFactory.nonEmpty

  override def startImpl(): Unit = {
    interpreterTaskFactory =
      Some(new InterpreterTaskFactory(defaultInterpreter))

    publishArtifact(interpreterTaskFactory.get)
  }

  override def stopImpl(): Unit = {
    interpreterTaskFactory = None
  }
}
