## Elasticsearch Client
The SumoLogic Elasticsearch library provides Elasticsearch bindings with a Scala DSL. Unlike other Scala libraries, this library targets the REST API. The REST API has a two primary advantages:
  1. Ability to upgrade Elasticsearch without the need to atomically also upgrade the client.
  2. Ability to use hosted Elasticsearch such as the version provided by [AWS](https://aws.amazon.com/elasticsearch-service/).

Along with a basic Elasticsearch client (elasticsearch-core), helper functionality for using Elasticsearch with Akka (elasticssearch-akka) and AWS (elasticsearch-aws) is also provided.

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
