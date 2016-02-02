## Elasticsearch Client
The SumoLogic Elasticsearch library provides Elasticsearch bindings with a Scala DSL. Unlike other Scala libraries, this library targets the REST API. The REST API has a two primary advantages:
  1. Ability to upgrade Elasticsearch without the need to atomically also upgrade the client.
  2. Ability to use hosted Elasticsearch such as the version provided by [AWS](https://aws.amazon.com/elasticsearch-service/).

Along with a basic Elasticsearch client (elasticsearch-core), helper functionality for using Elasticsearch with Akka (elasticssearch-akka) and AWS (elasticsearch-aws) is also provided.
