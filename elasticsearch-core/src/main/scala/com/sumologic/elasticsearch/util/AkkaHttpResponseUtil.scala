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

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import akka.http.scaladsl.model.ResponseEntity
import akka.stream.ActorMaterializer

object AkkaHttpResponseUtil {

  /**
    * Helper method for getting string out of Akka HTTP Entity
    * @param entity HTTP entity
    * @param streamTimeout timeout for Strict stream completion
    * @param ec implicit ExecutionContext
    */
  def entityToString(entity: ResponseEntity,
                     streamTimeout: FiniteDuration = 1000.millis)
                    (implicit ec: ExecutionContext = ExecutionContext.global,
                     materializer: ActorMaterializer): Future[String] = {
    entity.toStrict(streamTimeout).map(_.data.utf8String)
  }
}
