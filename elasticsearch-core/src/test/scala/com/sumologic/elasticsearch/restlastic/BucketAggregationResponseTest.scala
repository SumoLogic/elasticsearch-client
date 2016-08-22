package com.sumologic.elasticsearch.restlastic

import com.sumologic.elasticsearch.restlastic.RestlasticSearchClient.ReturnTypes.{BucketAggregationResponse, RawJsonResponse}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class BucketAggregationResponseTest extends WordSpec with Matchers with ScalaFutures {

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
      RawJsonResponse(aggregationsBucketsInsideBuckets).mappedTo[BucketAggregationResponse].aggregations._2 should be(expected)
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
