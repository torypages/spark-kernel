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

import com.typesafe.config.{ConfigFactory, Config}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpec, Matchers}
import scala.collection.JavaConversions._

class ImportLoaderSpec extends FunSpec with Matchers with MockitoSugar {
  describe("ImportLoader") {
    describe("#loadImports") {
      
      it("should load imports from config and pass them along to the interpreter"){
        val mockConfig = mock[Config]
        val mockInterpreter = mock[Interpreter]
        val mockImports = Seq("foo", "bar", "baz")
        val testPath: String = "interpreter.test.import"
        when(mockConfig.getStringList(testPath)).thenReturn(seqAsJavaList(mockImports))
        when(mockConfig.hasPath(testPath)).thenReturn(true)
        ImportLoader.loadImports(mockConfig, "test", mockInterpreter)
        verify(mockInterpreter).addImports(mockImports:_*)
      }
      
      it("should not invoke the interpreter if the are no values to import") {
        val mockInterpreter = mock[Interpreter]
        ImportLoader.loadImports(ConfigFactory.empty(), "test", mockInterpreter)
        verify(mockInterpreter, times(0)).addImports(anyString())
      }
    }
    
  }
}
