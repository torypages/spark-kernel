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

/**
 * Represents a collection of interpreters.
 *
 * @param interpreters The interpreters for this collection
 */
class InterpreterContainer(private val interpreters: Interpreter*)
  extends Seq[Interpreter]
{
  override def apply(idx: Int): Interpreter = interpreters(idx)

  override def length: Int = interpreters.length

  override def iterator: Iterator[Interpreter] = interpreters.iterator
}
