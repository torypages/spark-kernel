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

import java.io.File

import org.clapper.classutil.{ClassInfo, ClassFinder}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * Represents the manager of modules that will be loaded and executed by the
 * kernel.
 *
 * @param searchMainClassPath If true, will search the main classpath
 *                            containing the module manager (expensive)
 * @param extraClassPaths Any additional jars or zips to place on the class
 *                        path for lookup
 * @tparam T The interface to search for using the module manager
 */
class ModuleManager[T <: ModuleLike](
  private val searchMainClassPath: Boolean = true,
  private val extraClassPaths: Seq[String] = Nil
)(implicit classTag: ClassTag[T]) {
  import com.softwaremill.macwire.MacwireMacros._

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val moduleClassLoader =
    new ModuleClassLoader(Nil, this.getClass.getClassLoader)

  private lazy val mainClassFinder: Option[ClassFinder] =
    if (searchMainClassPath) Some(ClassFinder())
    else None
  private lazy val extraClassFinder: Option[ClassFinder] =
    if (extraClassPaths.nonEmpty) Some(ClassFinder(extraClassPaths.map(new File(_))))
    else None

  private lazy val allClasses: Iterator[ClassInfo] =
    mainClassFinder.map(_.getClasses()).getOrElse(Stream.empty[ClassInfo]).toIterator ++
    extraClassFinder.map(_.getClasses()).getOrElse(Stream.empty[ClassInfo])
  private lazy val allClassMap =
    ClassFinder.classInfoMap(allClasses.toIterator)

  /** Provides an interface for wiring dependencies together. */
  private val baseWired = wiredInModule(this)
  @volatile private var wired = baseWired

  /** Represents all available modules accessible by this manager. */
  private lazy val modules = {
    val moduleClassName = classTag.runtimeClass.getName
    println(s"Seeking instances of $moduleClassName")

    val _modules = ClassFinder
      .concreteSubclasses(moduleClassName, allClassMap)
      .toSeq

    // Print out results
    _modules.map(_.name).foreach(name => println(s"Found module: $name"))
    println(s"Found ${_modules.length} modules")

    _modules
  }

  /** Represents a mapping of class names to module information. */
  private lazy val moduleMap = modules.map(c => (c.name, c)).toMap

  /** Represents the full names of all available modules. */
  private lazy val moduleNames = moduleMap.keySet.toSeq

  def printModules(): Unit = modules.foreach { module =>
    println("-" * 80)
    println("Module Name: " + module.name)
    println("Interfaces: " + module.interfaces.mkString("|"))
    println("Fields: " + module.fields.mkString("|"))
    println("Methods: " + module.methods.mkString("|"))
    println("-" * 80)
  }

  /**
   * Loads all modules, returning their class representations.
   *
   * @return The collection of class representations
   */
  def loadAllModules(): Seq[Class[_]] = {
    val moduleLocations = modules.map(_.location.toURI.toURL)
    val existingLocations = moduleClassLoader.getURLs.toSeq

    // Add any missing paths to our class loader
    val newModuleLocations = moduleLocations.diff(existingLocations)
    newModuleLocations.foreach(moduleClassLoader.addURL)

    moduleNames.map(moduleClassLoader.loadClass)
  }

  /**
   * Starts the module with the specified class name.
   *
   * @param className The class name of the module to start
   *
   * @return The instantiated and started module
   */
  def startModule(className: String): AnyRef = {
    val moduleInstance = wired.wireClassInstanceByName(className) match {
      case m: ModuleLike => m
      case o =>
        val className = o.getClass.getName
        throw new ModuleException(s"Unknown module type: $className")
    }

    logger.info(s"Starting module: $className")
    moduleInstance.start()

    moduleInstance
  }
}
