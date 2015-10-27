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

package com.ibm.spark.interpreter

import com.typesafe.config.Config

import scala.collection.JavaConverters._

object ImportLoader {
  def loadImports(config: Config, interpreterName: String, interpreter: Interpreter) : Unit = {
    val importConfigPath: String = s"interpreter.${interpreterName}.import"
    if(config.hasPath(importConfigPath)) {
      val imports: Seq[String] = config.getStringList(importConfigPath).asScala
      interpreter.addImports(imports : _*)
    }
  }
}
