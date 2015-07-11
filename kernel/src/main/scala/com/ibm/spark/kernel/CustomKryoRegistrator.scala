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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.serializers.{MapSerializer, FieldSerializer, JavaSerializer}
import org.apache.spark.serializer.KryoRegistrator
import org.jruby.RubyBasicObject


/**
 * Created by senk on 7/11/15.
 */
class CustomKryoRegistrator extends KryoRegistrator {
  private class StubbedSerializer[T] extends Serializer[T] {
    override def read(kryo: Kryo, input: Input, `type`: Class[T]): T =
      `type`.cast(null)

    override def write(kryo: Kryo, output: Output, `object`: T): Unit = {}
  }

  private def dontSerializeClass[T](kryo: Kryo, className: String, klass: Class[T]): Unit = {
    kryo.register(Class.forName(className), new StubbedSerializer[T])
  }

  private def dontSerializeClass[T](kryo: Kryo, klass: Class[T]): Unit = {
    dontSerializeClass(kryo, klass.getName, klass)
  }

  private def removeFields[T](kryo: Kryo, className: String, klass: Class[T], fieldNames: String*): FieldSerializer[T] = {
    val classFromName = Class.forName(className)
    val serializer = new FieldSerializer[T](kryo, classFromName)

    fieldNames.foreach(serializer.removeField)

    kryo.register(classFromName, serializer)
    serializer
  }

  private def removeFields[T](kryo: Kryo, klass: Class[T], fieldNames: String*): FieldSerializer[T] = {
    removeFields(kryo, klass.getName, klass, fieldNames: _*)
  }

  override def registerClasses(kryo: Kryo): Unit = {
    def removeFieldsForClass[T](klass: Class[T], fieldNames: String*) =
      this.removeFields(kryo, klass, fieldNames: _*)

    println("REGISTERING KRYO WITH DEFAULT OF JAVA")
    kryo.setDefaultSerializer(classOf[IgnoreIllegalArgumentFieldSerializer[AnyRef]])

    /*removeFields(kryo,
      "sun.misc.Launcher$AppClassLoader",
      classOf[ClassLoader],
      "classes", "domains", "package2certs", "ucp", "pdcache"
    )*/
    dontSerializeClass(kryo,
      "sun.misc.Launcher$AppClassLoader",
      classOf[ClassLoader]
    )

    dontSerializeClass(kryo, classOf[org.apache.spark.api.java.JavaSparkContext])
    dontSerializeClass(kryo, classOf[java.lang.ThreadGroup])
    dontSerializeClass(kryo, classOf[java.lang.Thread])
    dontSerializeClass(kryo, classOf[java.lang.reflect.Method])

    removeFieldsForClass(classOf[org.jruby.Ruby],
      "javaSupport")

    val objectSpaceSerializer =
      removeFieldsForClass(classOf[org.jruby.runtime.ObjectSpace])
    objectSpaceSerializer.getField("identities").setSerializer({
      val mapSerializer = new MapSerializer
      mapSerializer.setGenerics(kryo, Array(
        classOf[java.lang.Long],
        Class.forName("org.jruby.runtime.ObjectSpace$IdReference")
      ))
      mapSerializer.asInstanceOf[Serializer[java.util.HashMap[_, _]]]
    })

    //dontSerializeClass(kryo, classOf[org.jruby.Ruby])

    removeFieldsForClass(classOf[org.jruby.java.invokers.InstanceMethodInvoker],
      "members", "nativeCall", "nativeCalls", "javaCallable", "javaCallables")
    removeFieldsForClass(classOf[org.jruby.java.invokers.StaticMethodInvoker],
      "members", "nativeCall", "nativeCalls")
    removeFieldsForClass(classOf[org.jruby.RubyClass],
      "reifiedClass")
    removeFieldsForClass(classOf[org.jruby.javasupport.JavaMethod],
      "parameterTypes", "boxedReturnType", "method")

    /*
    removeFieldsForClass(classOf[akka.event.BusLogging], "bus")

    removeFields(kryo,
      "akka.actor.LightArrayRevolverScheduler$TaskHolder",
      classOf[akka.actor.LightArrayRevolverScheduler],
      "executionContext"
    )

    removeFieldsForClass(classOf[io.netty.channel.nio.NioEventLoop],
      "selectedKeys", "selector", "thread")

    removeFieldsForClass(classOf[akka.dispatch.Dispatcher], "configurator")

    removeFieldsForClass(classOf[org.jruby.RubyThread], "threadImpl")

    // For failures on type void
    removeFieldsForClass(classOf[org.jruby.java.invokers.InstanceMethodInvoker],
      "members", "nativeCall", "nativeCalls", "javaCallable", "javaCallables")

    // For failures on type void
    removeFieldsForClass(classOf[java.lang.reflect.Method],
      "returnType", "clazz" /* null */)

    // For failures on null type
    removeFieldsForClass(classOf[org.jruby.javasupport.JavaMethod],
      "parameterTypes", "boxedReturnType", "method")

    // For failures on null type
    removeFieldsForClass(classOf[org.jruby.javasupport.binding.ClassInitializer],
      "javaClass")

    // For failures on null type
    removeFieldsForClass(classOf[org.jruby.java.invokers.StaticMethodInvoker],
      "members", "nativeCall", "nativeCalls")

    // For failures on null type
    removeFieldsForClass(classOf[org.jruby.java.invokers.SingletonMethodInvoker],
      "members", "nativeCall", "nativeCalls")

      // For failures on null type
    removeFieldsForClass(classOf[org.jruby.RubyClass], "reifiedClass")

    // For null
    removeFieldsForClass(classOf[org.jruby.util.collections.MapBasedClassValue[_]], "cache")
    */

  }
}
