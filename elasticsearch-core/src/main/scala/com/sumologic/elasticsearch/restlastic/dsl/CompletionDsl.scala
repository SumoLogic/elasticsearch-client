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
package com.sumologic.elasticsearch.restlastic.dsl

import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

trait CompletionDsl extends DslCommons with QueryDsl {

  case class Suggest(text: String, completion: CompletionWithContext) extends Query with RootObject {
    val _suggest = "suggest"
    val _text = "text"
    val _completion = "completion"

    override def toJson: Map[String, Any] = {
      Map(_suggest -> Map(
        _text -> text,
        _completion -> completion.toJson
      ))
    }
  }

  trait CompletionWithContext {
    val _field = "field"
    val _context = "context"
    val _size = "size"

    val field: String
    val size: Int
    def context: Iterable[(String, String)]

    def toJson: Map[String, Any] = Map(
      _field -> field,
      _context -> context.foldLeft(JObject()) {(k, v) => k ~ v },
      _size -> size
    )
  }

  case class Completion(field: String, size: Int, context: Map[String, String])
    extends Query with CompletionWithContext {

    def withAdditionalContext(newContext: (String, String)*) = {
      this.copy(context = context ++ newContext)
    }
  }

  /**
   * Support context filters with same key and different values
   * e.g. context: key->val1, key->val2
   */
  case class CompletionWithSeqContext(field: String, size: Int, context: Seq[(String, String)])
    extends Query with CompletionWithContext {

    def withAdditionalContext(newContext: (String, String)*) = {
      this.copy(context = context ++ newContext)
    }
  }
}


