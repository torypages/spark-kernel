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
import com.ibm.spark.magic.dependencies.DependencyMap
import com.ibm.spark.utils.LogLike

/**
 * Represents the module for creating a dependency map.
 */
class DependencyMapModule extends ModuleLike with LogLike {
  private var dependencyMap: Option[DependencyMap] = None

  override def isInitialized: Boolean = dependencyMap.nonEmpty

  override def startImpl(): Unit = {
    logger.debug("Initializing dependency map for magics")
    dependencyMap = Some(new DependencyMap)

    publishArtifact(dependencyMap.get)
  }

  override def stopImpl(): Unit = {
    dependencyMap = None
  }
}
