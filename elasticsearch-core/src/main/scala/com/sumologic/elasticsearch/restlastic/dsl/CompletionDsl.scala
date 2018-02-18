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

trait CompletionDsl extends DslCommons with QueryDsl {

  case class SuggestRoot(textOpt: Option[String], suggestions: List[Suggestion]) extends Query with RootObject {
    val _suggest = "suggest"
    val _text = "text"


    override def toJson: Map[String, Any] = {
      val globalTextJson = if (textOpt.isDefined) Map(_text -> textOpt.get) else Map.empty

      globalTextJson ++ Map(_suggest -> suggestions.map(p => p.name -> p.toJson).toMap)
    }
  }

  case class Suggestion(name: String, textOpt: Option[String], termOpt: Option[SuggestionTerm], completionOpt: Option[Completion]) extends Query {
    val _text = "text"
    val _term = "term"
    val _completion = "completion"

    override def toJson: Map[String, Any] = {
      val textJson = if (textOpt.isDefined) Map(_text -> textOpt.get) else Map.empty[String, Any]
      val termJson = if (termOpt.isDefined) Map(_term -> termOpt.get.toJson) else Map.empty[String, Any]
      val completionJson = if (completionOpt.isDefined) Map(_completion -> completionOpt.get.toJson) else Map.empty[String, Any]
      termJson ++ textJson ++ completionJson
    }
  }

  case class SuggestionTerm(fieldName: String) extends Query {
    val _field = "field"

    override def toJson: Map[String, Any] = {
      Map(_field -> fieldName)
    }
  }

  case class Completion(field: String, size: Int, name: String, contexts: List[Context]) extends Query {
    val _field = "field"
    val _contexts = "contexts"
    val _size = "size"

    override def toJson: Map[String, Any] = Map(
      _field -> field,
      _contexts -> Map(name -> contexts.map(_.toJson)),
      _size -> size
    )
  }

  case class Context(values: List[String]) extends Query {
    val _context = "context"

    override def toJson: Map[String, Any] = values.flatMap(value => Map(_context -> value)).toMap
  }

}


