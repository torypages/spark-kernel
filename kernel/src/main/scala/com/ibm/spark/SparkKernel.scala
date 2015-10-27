/*
 * Copyright 2014 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.spark

import com.ibm.spark.boot.layer._
import com.ibm.spark.boot.{CommandLineOptions, KernelBootstrap}
import com.ibm.spark.kernel.BuildInfo
import com.ibm.spark.kernel.module.ModuleManager
import org.clapper.classutil.ClassFinder

object SparkKernel extends App {
  private val options = new CommandLineOptions(args)

  val filteredClassPath = ClassFinder.classpath
    .map(_.getPath).filter(_.contains("spark-kernel"))
  //println("Classpath: " + ClassFinder.classpath.mkString("\n"))
  //println("-----------------")
  //println("Checking: " + filteredClassPath.mkString("\n"))

  val beforeTime = System.currentTimeMillis()
  val moduleManager = new ModuleManager[com.ibm.spark.interpreter.Interpreter](
    searchMainClassPath = false,
    extraClassPaths = filteredClassPath
  )
  val afterTime = System.currentTimeMillis()
  //println("Time taken: " + ((afterTime - beforeTime) / 1000.0) + "s")

  //modules.foreach(println)
  val modules = moduleManager.loadAllModules()
  modules.foreach { module =>
    println("Module Name: " + module.getName)
    module.getDeclaredConstructors.foreach { constructor =>
      println("Module constructor: " + constructor.getParameterTypes.mkString(","))
    }
  }

  //moduleManager.printModules()
  Thread.sleep(1000)
  System.exit(0)

  if (options.help) {
    options.printHelpOn(System.out)
  } else if (options.version) {
    println(s"Kernel Version:       ${BuildInfo.version}")
    println(s"Build Date:           ${BuildInfo.buildDate}")
    println(s"Scala Version:        ${BuildInfo.scalaVersion}")
    println(s"Apache Spark Version: ${BuildInfo.sparkVersion}")
  } else {
    (new KernelBootstrap(options.toConfig)
      with StandardBareInitialization
      with StandardComponentInitialization
      with StandardHandlerInitialization
      with StandardHookInitialization)
      .initialize()
      .waitForTermination()
      .shutdown()
  }
}
