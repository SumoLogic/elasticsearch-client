/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.sumologic.elasticsearchsix.restlastic.dsl

import org.json4s.Extraction._
import org.json4s._
import org.json4s.native.JsonMethods._

trait DslCommons {

  trait EsOperation {
    def toJson: Map[String, Any]
  }

  trait FieldType {
    val rep: String
  }

  object EsOperation {
    implicit val formats = org.json4s.DefaultFormats

    def compactJson(map: Map[String, Any]) = compact(render(decompose(map)))
  }

  trait RootObject extends EsOperation {

    import EsOperation._

    def toJsonStr: String = compactJson(toJson)
  }

  case object EmptyObject extends RootObject {
    override def toJson: Map[String, Any] = Map()
  }

  case object NoOp extends RootObject {
    override def toJson: Map[String, Any] = throw new UnsupportedOperationException

    override def toJsonStr = ""
  }

  abstract class SingleField(field: String, value: EsOperation) extends EsOperation {
    override def toJson: Map[String, Any] = Map(
      field -> value.toJson
    )
  }

  case class Index(name: String)

  case class Type(name: String)

  case class Name(name: String)

}


