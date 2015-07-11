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

package com.ibm.spark.kernel

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.{JavaSerializer, FieldSerializer}

import scala.util.Try

class IgnoreIllegalArgumentFieldSerializer[T](kryo: Kryo, klass: Class[T]) extends Serializer[T] {
  private val Debug = true
  private val fieldSerializer: Option[FieldSerializer[T]] =
    Try(kryo.newSerializer(
      classOf[FieldSerializer[T]], klass
    )).toOption.map(_.asInstanceOf[FieldSerializer[T]])

  private val javaSerializer: JavaSerializer = kryo.newSerializer(
    classOf[JavaSerializer], klass
  ).asInstanceOf[JavaSerializer]

  override def read(kryo: Kryo, input: Input, `type`: Class[T]): T = {
    if (fieldSerializer.nonEmpty) fieldSerializer.get.read(kryo, input, `type`)
    else `type`.cast(javaSerializer.read(kryo, input, `type`))
  }

  override def write(kryo: Kryo, output: Output, `object`: T): Unit = try {
    if (fieldSerializer.nonEmpty)
      fieldSerializer.get.write(kryo, output, `object`)
    else
      javaSerializer.write(kryo, output, `object`)
  } catch {
    case ex: IllegalArgumentException => /* Ignore potential void class */
      if (Debug) Console.err.println("IGNORING: " + ex.getLocalizedMessage)
    case throwable: Throwable => throw throwable
  }
}

