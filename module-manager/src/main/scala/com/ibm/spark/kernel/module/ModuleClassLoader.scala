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

import java.net.{URL, URLClassLoader}

/**
 * Represents a class loader used to manage classes used as modules.
 *
 * @param urls The initial collection of URLs pointing to paths to load
 *             module classes
 * @param parentLoader The parent loader to use as a fallback to load module
 *                     classes
 */
class ModuleClassLoader(
  private val urls: Seq[URL],
  private val parentLoader: ClassLoader
) extends URLClassLoader(urls.toArray, parentLoader) {
  /**
   * Adds a new URL to be included when loading module classes.
   *
   * @param url The url pointing to the new module classes to load
   */
  override def addURL(url: URL): Unit = super.addURL(url)
}
