/*
 * Copyright 2015 IBM Corp.
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
libraryDependencies += "org.clapper" %% "classutil" % "1.0.3" // New BSD

libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, scalaMajor)) if scalaMajor == 10 => Seq(
    "com.softwaremill.macwire" %% "macros" % "0.7.3",
    "com.softwaremill.macwire" %% "runtime" % "0.7.3"
  )
  case Some((2, scalaMajor)) if scalaMajor == 11 => Seq(
    "com.softwaremill.macwire" %% "macros" % "1.0.5",
    "com.softwaremill.macwire" %% "runtime" % "1.0.5"
  )
  case Some((2, scalaMajor)) if scalaMajor < 10 =>
    throw new Throwable("Scala version below 2.10 not supported!")
  case Some((scalaMega, scalaMajor)) =>
    throw new Throwable(s"Unknown Scala version: $scalaMega.$scalaMajor")
})
