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
import com.ibm.spark.kernel.protocol.v5.magic.PostProcessor
import com.ibm.spark.utils.LogLike

/**
 * Represents the module to create the magic parser.
 *
 * @param defaultInterpreter The default interpreter to use for the post
 *                           processor
 */
class PostProcessorModule(
  private val defaultInterpreter: DefaultInterpreter
) extends ModuleLike with LogLike {
  private var postProcessor: Option[PostProcessor] = None

  override def isInitialized: Boolean = postProcessor.nonEmpty

  override def startImpl(): Unit = {
    postProcessor = Some(new PostProcessor(defaultInterpreter))

    publishArtifact(postProcessor.get)
  }

  override protected def stopImpl(): Unit = {
    postProcessor = None
  }
}
