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

    import scala.reflect.internal.annotations.compileTimeOnly
    import scala.reflect.macros.Context
    import scala.language.experimental.macros
    import scala.annotation.StaticAnnotation

    /**
     * Represents a macro that injects appropriate getters and setters into the
     * annotated trait such that it can be treated as a dependency.
     *
     * @param variableName The name of the variable to provide getter/setter
     * @param className The class name of the variable
     */
    @compileTimeOnly("Enable macro paradise to expand macro annotations")
    class Dependency(
      variableName: String,
      className: String
    ) extends StaticAnnotation {
      def macroTransform(annottees: Any*): Any = macro Dependency.impl
    }

    object Dependency {
      def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
        import c.universe._
        import Flag._
        println("Starting macro")
        println("Prefix: " + c.prefix)

        val q"new Dependency(..$params)" = c.prefix.tree
        println("Params: " + params)

        val (variableName: String, className: String) = params match {
          case p: List[_] =>
            val stringParams = p.map(_.toString.replace("\"", ""))
            (stringParams(0), stringParams(1))
          case _ => c.abort(
            c.enclosingPosition,
            "Required arguments are (variable name: String) (class name: String)"
          )
        }

        println("Variable name: " + variableName)
        println("Class name: " + className)

        def modifiedTrait(traitDecl: ClassDef) = {
          val (name) = try {
            val q"trait $name" = traitDecl
            (name)
          } catch {
            case _: MatchError => c.abort(
              c.enclosingPosition,
              "Annotation is only supported on trait"
            )
          }
          println("Trait name: " + name)

          val actualVariableName = variableName.toLowerCase
          println("Actual variable name: " + actualVariableName)
          val actualVariableClass = newTypeName(className)
          println("Actual class name: " + actualVariableClass)

          val internalVariableName = newTermName("_" + actualVariableName)

          println("Internal variable name: " + internalVariableName)

          val getterName = newTermName(actualVariableName)
          println("Getter name: " + getterName)
          val setterName = newTermName(actualVariableName + "_=")
          println("Setter name: " + setterName)
          val setterVariableName = newTermName("new" + actualVariableName.capitalize)

          println("Setter variable name: " + setterVariableName)

          val generatedTrait = q"""
            trait $name {
              private var $internalVariableName: $actualVariableClass = _

              def $setterName($setterVariableName: $actualVariableClass) =
                $internalVariableName = $setterVariableName

              def $getterName: $actualVariableClass = $internalVariableName
            }
          """

          println("Generated trait: " + generatedTrait)

          c.Expr[Any](generatedTrait)
        }

        annottees.map(_.tree) match {
          case (traitDecl: ClassDef) :: Nil => modifiedTrait(traitDecl)
          case _ => c.abort(c.enclosingPosition, "Invalid annottee")
        }
      }
    }
