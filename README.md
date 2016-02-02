## Elasticsearch Client
The SumoLogic Elasticsearch library provides Elasticsearch bindings with a Scala DSL. Unlike other Scala libraries like [elastic4s](https://github.com/sksamuel/elastic4s) this library targets the REST API. The REST API has a two primary advantages:
  1. Ability to upgrade Elasticsearch without the need to atomically also upgrade the client.
  2. Ability to use hosted Elasticsearch such as the version provided by [AWS](https://aws.amazon.com/elasticsearch-service/).

Along with a basic Elasticsearch client (`elasticsearch-core`), helper functionality for using Elasticsearch with Akka (`elasticssearch-akka`) and AWS (`elasticsearch-aws`) is also provided. The goal of the DSL is to keep it as simple as possible, occasionally sacrifing some end-user boilerplate to maintain a DSL that is easy to modify and add to. The DSL attempts to be type-safe in that it should be impossible to create an invalid Elasticsearch query.

## Usage
All client methods return futures that can be composed to perform multiple actions.

### Basic Usage
```scala
val restClient = new RestlasticSearchClient(new StaticEndpoint(new Endpoint(host, port)))
val index = Index("index-name")
val tpe = Type("type")
val indexFuture = for {
  _ <- restClient.createIndex(index)
  indexResult <- restClient.index(index, tpe, Document("docId", Map("text" -> "Hello World!")))
}
Await.result(indexFuture, 10.seconds)
// Need to wait for a flush. In ElasticsearchIntegrationTest, you can just call "refresh()"
Thread.sleep(2000)
restClient.query(index, tpe, QueryRoot(TermQuery("text", "Hello World!"))).map { res =>
  println(res.sourceAsMap) // List(Map(text -> Hello World))
}
```
https://github.com/SumoLogic/elasticsearch-client/blob/master/elasticsearch-core/src/test/scala/com/sumologic/elasticsearch/restlastic/RestlasticSearchClientTest.scala provides other basic examples.

### Using the BulkIndexerActor
#### NOTE: to use the BulkIndexerActor you must add a dependency to `elasticsearch-akka`.
The BulkIndexer actor provides a single-document style interface that actually delegates to the bulk API. This allows you to keep your code simple while still getting the performance benefits of bulk inserts and updates. The BulkIndexerActor has two configurable parameters:
  * `maxDocuments`: The number of documents at which a bulk request will be flushed
  * `flushDuration`: If the `maxDocuments` limit is not hit, it will flush after `flushDuration`.
```scala
val restClient: RestlasticSearchClient = ...
val (index, tpe) = (Index("i"), Type("t"))
// Designed for potentially dynamic configuration:
val config = new BulkConfig(
  flushDuration = () => FiniteDuration(2, TimeUnit.Seconds),
  maxDocuments = () => 2000)
val bulkActor = context.actorOf(BulkIndexerActor.props(restClient, config))
val sess = BulkSession.create()
val resultFuture = bulkActor ? CreateRequest(sess, index, tpe, Document("id", Map("k" -> "v")))
val resultFuture2 = bulkActor ? CreateRequest(sess, index, tpe, Document("id", Map("k" -> "v")))
// The resultFuture contains a `sessionId` you can use to match up replies with requests assuming you do not use
// the ask pattern as above.
// The two requests will be batched into a single bulk request and sent to Elasticsearch 
```

You can also use the Bulk api directly via the REST client:
```scala
restClient.bulkIndex(index, tpe, Seq(doc1, doc2, doc3))
```

### Usage With AWS

One common way to configure AWS Elasticsearch is with IAM roles. This requires you to sign every request you send to Elasticsearch with your use key. The `elasticsearch-aws` module includes a request signer for this purpose:
```scala
import com.sumologic.elasticsearch.util.AwsRequestSigner
import com.amazonaws.auth.AWSCredentials
val awsCredentials = _ // Credentials for the AWS use that has permissions to access Elasticsearch
val signer = new AwsRequestSigner(awsCredentials, "REGION", "es")
// You can also create your own dynamic endpoint class based off runtime configuration or the AWS API.
val endpoint = new StaticEndpoint(new Endpoint("es.blahblahblah.amazon.com", 443))
val restClient = new RestlasticSearchClient(endpoint, Some(signer))
```
`restClient` will now have every request automatically signed with your credentials.
