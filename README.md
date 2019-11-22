[![Build Status](https://travis-ci.org/SumoLogic/elasticsearch-client.svg?branch=master)](https://travis-ci.org/SumoLogic/elasticsearch-client)
[![codecov.io](https://codecov.io/github/SumoLogic/elasticsearch-client/coverage.svg?branch=master)](https://codecov.io/github/SumoLogic/elasticsearch-client?branch=master)
[![Join the chat at https://gitter.im/SumoLogic/elasticsearch-client](https://badges.gitter.im/SumoLogic/elasticsearch-client.svg)](https://gitter.im/SumoLogic/elasticsearch-client?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Elasticsearch Client

The Sumo Logic Elasticsearch library provides Elasticsearch bindings with a Scala DSL. Unlike other Scala libraries like [elastic4s](https://github.com/sksamuel/elastic4s) this library targets the REST API. The REST API has a two primary advantages:
  1. Ability to upgrade Elasticsearch without the need to atomically also upgrade the client.
  2. Ability to use hosted Elasticsearch such as the version provided by [AWS](https://aws.amazon.com/elasticsearch-service/).

_This project is currently targeted at Elasticsearch 6.0.x. For ES 2.3 compatibility see version 3 (`release-3.*` branch)._

Along with a basic Elasticsearch client (`elasticsearch-core`), helper functionality for using Elasticsearch with Akka (`elasticssearch-akka`) and AWS (`elasticsearch-aws`) is also provided. The goal of the DSL is to keep it as simple as possible, occasionally sacrifing some end-user boilerplate to maintain a DSL that is easy to modify and add to. The DSL attempts to be type-safe in that it should be impossible to create an invalid Elasticsearch query. Rather than be as compact as possible, the DSL aims to closely reflect the JSON it generates when reasonable. This makes it easier discover how to access functionality than a traditional maximally compact DSL.
## Install / Download
The library components are offered a la carte:
* `elasticsearch-core_2.11` contains the basic Elasticsearch client and typesafe DSL
* `elasticsearch-aws_2.11` contains utilities for using [AWS Hosted Elasticsearch](https://aws.amazon.com/elasticsearch-service/).
* `elasticsearch-akka_2.11` contains Actors to use with Akka & Akka Streams
```
    <dependency>
      <groupId>com.sumologic.elasticsearch</groupId>
      <artifactId>elasticsearch-core_2.11</artifactId>
      <version>6.0.0</version>
    </dependency>

    <dependency>
      <groupId>com.sumologic.elasticsearch</groupId>
      <artifactId>elasticsearch-aws_2.11</artifactId>
      <version>6.0.0</version>
    </dependency>

    <dependency>
      <groupId>com.sumologic.elasticsearch</groupId>
      <artifactId>elasticsearch-akka_2.11</artifactId>
      <version>6.0.0</version>
    </dependency>
  ```
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
val awsCredentials = _ // Credentials for the AWS user that has permissions to access Elasticsearch
val signer = new AwsRequestSigner(awsCredentials, "REGION", "es")
// You can also create your own dynamic endpoint class based off runtime configuration or the AWS API.
val endpoint = new StaticEndpoint(new Endpoint("es.blahblahblah.amazon.com", 443))
val restClient = new RestlasticSearchClient(endpoint, Some(signer))
```
`restClient` will now sign every request automatically with your AWS credentials.

## Contributing

Sumo Logic Elasticsearch uses Maven and the Maven GPG Plug-in for builds and testing. After cloning the repository make sure you have a GPG key created.  Then run `maven clean install`.


### [Dev] Building

To build project in default Scala version:
```
./gradlew build
```

To build project in any supported Scala version:
```
./gradlew build -PscalaVersion=2.12.8
```

### [Dev] Testing

Tests in this project are run against local Elasticsearch servers es23 es63.

For testing, change your consumer `pom.xml` or `gradle.properties` to depend on the `SNAPSHOT` version generated.

### [Dev] Managing Scala versions

This project supports multiple versions of Scala. Supported versions are listed in `gradle.properties`.
- `supportedScalaVersions` - list of supported versions (Gradle prevents building with versions from 
outside this list)
- `defaultScalaVersion` - default version of Scala used for building - can be overridden with `-PscalaVersion`

### [Dev] How to release new version
1. Make sure you have all credentials - access to `Open Source` vault in 1Password.
    1. Can login as `sumoapi` https://oss.sonatype.org/index.html
    2. Can import and verify the signing key:
        ```
        gpg --import ~/Desktop/api.private.key
        gpg-agent --daemon
        touch a
        gpg --use-agent --sign a
        gpg -k
        ```
    3. Have nexus and signing credentials in `~/.gradle/gradle.properties`
        ```
        nexus_username=sumoapi
        nexus_password=${sumoapi_password_for_sonatype_nexus}
        signing.gnupg.executable=gpg
        signing.gnupg.keyName=${id_of_imported_sumoapi_key}
        signing.gnupg.passphrase=${password_for_imported_sumoapi_key}
        ```
2. Remove `-SNAPSHOT` suffix from `version` in `build.gradle`
3. Make a release branch with Scala version and project version, ex. `elasticsearch-client-7.0.0-M19`:
    ```
    export RELEASE_VERSION=elasticsearch-client-7.0.0-M19
    git checkout -b ${RELEASE_VERSION}
    git add build.gradle
    git commit -m "[release] ${RELEASE_VERSION}"
    ```
4. Perform a release in selected Scala versions:
    ```
    ./gradlew build publish -PscalaVersion=2.11.12
    ./gradlew build publish -PscalaVersion=2.12.8
    ```
5. Go to https://oss.sonatype.org/index.html#stagingRepositories, search for com.sumologic and release your repo. 
NOTE: If you had to login, reload the URL. It doesn't take you to the right page post-login
6. Update the `README.md` with the new version and set upcoming snapshot `version` 
in `build.gradle`, ex. `7.0.0-M20-SNAPSHOT` 
7. Commit the change and push as a PR:
    ```
    git add build.gradle README.md CHANGELOG.md
    git commit -m "[release] Updating version after release ${RELEASE_VERSION}"
    git push
    ```
