/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ.analyzers.runners

import scala.reflect.runtime.{universe => ru}
import ru._

object ReflectionUtils {
  def getFieldValue(obj: Any, fieldName: String): Option[Any] = {
    val mirror = runtimeMirror(obj.getClass().getClassLoader())
    val clazz = mirror.classSymbol(obj.getClass())

    val fieldTermSymbol = clazz.info.decls.find(_.name.decodedName.toString == fieldName)

    if (fieldTermSymbol.isDefined) {
      Some(mirror.reflect(obj).reflectField(fieldTermSymbol.get.asTerm).get)
    } else {
      None
    }
  }
}
