/*
 * Copyright 2014 IBM Corp.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.ibm.spark.annotations

import org.scalatest.{OneInstancePerTest, Matchers, FunSpec}

class DependencySpec extends FunSpec with Matchers with OneInstancePerTest {

  describe("@Dependency") {
    it("should generate a setter with the name specified") {
      @Dependency("someFakeField", "Int") trait TestTrait

      class A extends TestTrait
      val a = new A

      println("TestTrait")
      classOf[TestTrait].getDeclaredMethods.foreach(println)

      println("A")
      classOf[A].getDeclaredMethods.foreach(println)

      // This line fails
      //a.someFakeField = 3
    }

    it("should generate a getter with the name specified") {
      //@Dependency("b", "scala.Int") trait TestTrait
    }
  }
}
