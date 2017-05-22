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
package com.sumologic.elasticsearch_test

import java.io.File

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.Node
import java.util.Collections

import org.elasticsearch.node.internal.InternalSettingsPreparer
import org.elasticsearch.transport.Netty4Plugin

import scala.util.{Random, Try}
import scala.collection.JavaConversions._

/**
 * Created by Russell Cohen on 11/2/15.
  *
  * A mixin trait for UTs requiring Elasticsearch. The trait will manage a global Elasticsearch instance across all tests.
  *
  * You should use the index `IndexName` provided by the trait as it is guaranteed to be absent when the test starts
  * and will be cleaned up when the test is complete.
 */

trait ElasticsearchIntegrationTest extends BeforeAndAfterAll {
  this: Suite =>

  import ElasticsearchIntegrationTest._

  lazy val endpoint = globalEndpoint

  lazy val IndexName = allocateNewIndexName

  def allocateNewIndexName = synchronized {
    s"index-${r.nextLong()}"
  }

  override def beforeAll(): Unit = {
    esNode
    client
    endpoint
    Try(delete(IndexName))
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    Try(delete(IndexName))
    super.afterAll()
    esNode.close()
    client.close()
  }

  def refresh(): Unit = esNode.client().admin().indices().prepareRefresh().execute().actionGet()

  def delete(index: String) = Try(client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet())
}

object ElasticsearchIntegrationTest {
  private val r = new Random()
  private lazy val esNodeSettings = Settings.builder().
    put("path.home", createTempDir("elasticsearch-test")).
    put("transport.type", "netty4").
    put("http.type", "netty4").
    put("http.enabled", "true").
    build()
  private lazy val esNode = new PluginNode(esNodeSettings).start()
  lazy val client = esNode.client()
  lazy val globalEndpoint = {
    val nodeInfos = client.admin().cluster().prepareNodesInfo().clear().setSettings(true).setHttp(true).get()
    val nodeAddress =
      nodeInfos.getNodes.map(_.getHttp.address()).head.publishAddress().asInstanceOf[InetSocketTransportAddress]
    val host = nodeAddress.address().getHostString
    val port = nodeAddress.address().getPort
    (host, port)
  }


  def createTempDir(name: String = "TempDir") = {
    val tempFile = File.createTempFile(name, f"${System.currentTimeMillis()}")
    val tempDir = new File(
      f"${
        tempFile.getParentFile.getAbsolutePath
      }${File.separator}$name-file-${System.currentTimeMillis()}"
    )
    tempDir.mkdir()
    tempFile.delete()
    tempDir.deleteOnExit()
    tempDir
  }
}

class PluginNode(settings: Settings)
  extends Node(InternalSettingsPreparer.prepareEnvironment(settings, null),
    Collections.singletonList(classOf[Netty4Plugin]))

