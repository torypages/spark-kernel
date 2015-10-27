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

import com.ibm.spark.kernel.module.ModuleLike
import com.ibm.spark.utils.{ResponseMap, LogLike}

/**
  * Represents the module for creating a response map.
  */
class ResponseMapModule extends ModuleLike with LogLike {
  private var responseMap: Option[ResponseMap] = None

  override def isInitialized: Boolean = responseMap.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Creating response map")
    responseMap = Some(new ResponseMap)

    publishArtifact(responseMap.get)
  }

  override def stopImpl(): Unit = {
    responseMap = None
  }
}
