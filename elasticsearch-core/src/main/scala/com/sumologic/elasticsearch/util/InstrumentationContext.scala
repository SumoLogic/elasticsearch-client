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
package com.sumologic.elasticsearch.util
import scala.collection.mutable
/**
 * Created by russell on 1/29/16.
 * Track timing for multiple named events
 */
class InstrumentationContext {
  private val resultMap = mutable.Map[String, Long]().withDefaultValue(0L)
  def measureAction[T](action: String)(fn: => T): T = {
    val startTime = System.currentTimeMillis()
    val res = fn
    val endTime = System.currentTimeMillis()
    resultMap.put(action, endTime - startTime + resultMap(action))
    res
  }

  def measurementsString: String = {
    resultMap.map({
      case (actionName, measurement) => s"$actionName='${measurement}ms'"
    }).mkString(", ")
  }

}
