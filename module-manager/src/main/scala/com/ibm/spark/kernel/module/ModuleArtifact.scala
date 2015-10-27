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

package com.ibm.spark.kernel.module

/**
 * Represents an artifact created by a module to be used by other modules
 * as a dependency.
 *
 * @param value The value encapsulated by this artifact
 * @param name The name of the artifact to use when more than one of the
 *             same type exists
 */
case class ModuleArtifact(
  value: AnyRef,
  name: String
) {
  val `class` = value.getClass
}
