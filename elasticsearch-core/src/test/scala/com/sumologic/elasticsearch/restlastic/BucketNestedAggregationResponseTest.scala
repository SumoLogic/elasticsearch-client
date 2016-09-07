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
package com.sumologic.elasticsearch.restlastic

import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{BucketNestedAggregationResponse, RawJsonResponse}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class BucketNestedAggregationResponseTest extends WordSpec with Matchers with ScalaFutures {

  "BucketAggregationResponse" should {
    "parse buckets inside buckets" in {
      val expected = Map(
        "doc_count_error_upper_bound" -> 0,
        "sum_other_doc_count" -> 0,
        "buckets" -> List(Map(
          "key" -> "honda",
          "doc_count" -> 3,
          "color" -> Map(
            "doc_count_error_upper_bound" -> 0,
            "sum_other_doc_count" -> 0,
            "buckets" -> List(Map(
              "key" -> "black",
              "doc_count" -> 2), Map(
              "key" -> "red",
              "doc_count" -> 1)))))
      )
      RawJsonResponse(aggregationsBucketsInsideBuckets).mappedTo[BucketNestedAggregationResponse].aggregations._2 should be(expected)
    }
  }

  val aggregationsBucketsInsideBuckets =
    """{
    	"took": 3,
    	"timed_out": false,
    	"_shards": {
    		"total": 12,
    		"successful": 12,
    		"failed": 0
    	},
    	"hits": {
    		"total": 45,
    		"max_score": 0.0,
    		"hits": []
    	},
    	"aggregations": {
    		"make": {
    			"doc_count_error_upper_bound": 0,
    			"sum_other_doc_count": 0,
    			"buckets": [{
    				"key": "honda",
    				"doc_count": 3,
    				"color": {
    					"doc_count_error_upper_bound": 0,
    					"sum_other_doc_count": 0,
    					"buckets": [{
    						"key": "black",
    						"doc_count": 2
    					}, {
    						"key": "red",
    						"doc_count": 1
    					}]
    				}
    			}]
    		}
    	}
    }"""
}
