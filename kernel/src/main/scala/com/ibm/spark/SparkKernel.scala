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

import com.esotericsoftware.minlog.Log
import com.ibm.spark.boot.layer._
import com.ibm.spark.boot.{CommandLineOptions, KernelBootstrap}
import com.ibm.spark.kernel.{CustomKryoRegistrator, BuildInfo}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import org.jruby.CompatVersion
import org.jruby.RubyInstanceConfig.CompileMode
import org.jruby.embed.{ScriptingContainer, LocalContextScope, LocalVariableBehavior}

object SparkKernel extends App {
  private val options = new CommandLineOptions(args)

  println("Setting up Spark configuration")
  val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
  sparkConf.set("spark.kryo.registrator", classOf[CustomKryoRegistrator].getName)
  println(sparkConf.toDebugString)

  println("Creating Spark context")
  val sparkContext = new SparkContext(sparkConf)

  println("Adding JRuby jars to Spark Cluster")
  sparkContext.addJar("/Users/senk/.ivy2/cache/org.jruby/jruby-complete/jars/jruby-complete-1.7.21.jar")

  println("Initializing JRuby")
  val scriptingContainer = new ScriptingContainer(LocalContextScope.THREADSAFE, LocalVariableBehavior.TRANSIENT)
  scriptingContainer.setCompatVersion(CompatVersion.RUBY2_0)
  scriptingContainer.setCompileMode(CompileMode.OFF)

  println("Binding Spark Context into JRuby")
  scriptingContainer.put("sc", new JavaSparkContext(sparkContext))

  // -Xss10m -Dsun.io.serialization.extendedDebugInfo=true
  def run(code: String) = {
    println("Running code: " + code)
    println("RESULT: " + scriptingContainer.runScriptlet(code))
  }

  //Log.DEBUG()
  run("@x = sc.parallelize([1, 2, 3, 4])")
  run("@x.reduce(lambda { |a, b| return a + b })")

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
