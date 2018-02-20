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

import java.net.URLEncoder
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Host, RawHeader}
import akka.util.ByteString
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSSessionCredentials}
import com.amazonaws.internal.StaticCredentialsProvider
import com.sumologic.elasticsearch.restlastic.RequestSigner

import scala.concurrent.Await

/**
 * Sign AWS requests following the instructions at http://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
 * @param awsCredentialsProvider Interface for providing AWS credentials
 * @param region Region to connect to
 * @param service Service to connect to (eg. "es" for elasticsearch) [http://docs.aws.amazon.com/general/latest/gr/rande.html]
 */
class AwsRequestSigner(awsCredentialsProvider: AWSCredentialsProvider, region: String, service: String) extends RequestSigner {

  val Algorithm = "AWS4-HMAC-SHA256"
  require(awsCredentialsProvider.getCredentials != null, "awsCredentialsProvider must return non null awsCredentials.")

  /**
   * Sign AWS requests following the instructions at http://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
   * @param awsCredentials AWS creds to sign with
   * @param region Region to connect to
   * @param service Service to connect to (eg. "es" for elasticsearch) [http://docs.aws.amazon.com/general/latest/gr/rande.html]
   */
  def this(awsCredentials: AWSCredentials, region: String, service: String) = {
    this(new StaticCredentialsProvider(awsCredentials), region, service)
  }

  private def awsCredentials: AWSCredentials = awsCredentialsProvider.getCredentials

  def withAuthHeader(httpRequest: HttpRequest): HttpRequest = {
    val (dateTimeStr, dateStr) = currentDateStrings

    val withDateAndHost = completedRequest(httpRequest, dateTimeStr)

    val headerValue = s"$Algorithm Credential=${awsCredentials.getAWSAccessKeyId}/${credentialScope(dateStr)}, SignedHeaders=${signedHeaders(withDateAndHost)}" +
      s", Signature=${signature(withDateAndHost, dateTimeStr, dateStr)}"


    val withAuth = withDateAndHost.addHeader(RawHeader("Authorization", headerValue))

    // Append the session key, if session credentials were provided
    addSessionToken(withAuth)
  }

  val xAmzSecurityToken = "X-Amz-Security-Token"

  def addSessionToken(httpRequest: HttpRequest): HttpRequest = awsCredentials match {
    case (sc: AWSSessionCredentials) =>
      httpRequest.addHeader(RawHeader(xAmzSecurityToken, sc.getSessionToken))
    case _ => httpRequest
  }

  private[util] def completedRequest(httpRequest: HttpRequest, dateTimeStr: String): HttpRequest = {
    // Add a date and host header, but only if they aren't already there
    val headerNames = Set(xAmzDate.toLowerCase, "date")

    val withDate = if (headerNames.exists(name => httpRequest.getHeader(name).isPresent)) {
      httpRequest
    } else {
      httpRequest.addHeader(RawHeader(xAmzDate, dateTimeStr))
    }


    val withHost = if (httpRequest.getHeader("host").isPresent) {
      withDate
    } else {
      val hostHeader = Host(withDate.uri.authority.host, httpRequest.uri.authority.port)
      withDate.addHeader(hostHeader)
    }

    withHost
  }

  private[util] def stringToSign(httpRequest: HttpRequest, dateTimeStr: String, dateStr: String): String = {
    s"$Algorithm\n" +
      s"$dateTimeStr\n" +
      s"${credentialScope(dateStr)}\n" +
      s"${canonicalRequestHash(httpRequest)}"
  }

  private[util] def createCanonicalRequest(httpRequest: HttpRequest): String = {
    f"${httpRequest.method.name}\n" +
      f"${canonicalUri(httpRequest)}\n" +
      f"${canonicalQueryString(httpRequest)}\n" +
      f"${canonicalHeaders(httpRequest)}\n" +
      f"${signedHeaders(httpRequest)}\n" +
      f"${hexOf(hashedPayloadByteArray(httpRequest))}"
  }


  // Protected so that it can be overridden in tests
  protected def currentDateStrings: (String, String) = {
    val cal = Calendar.getInstance()
    val dfmT = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'")
    dfmT.setTimeZone(TimeZone.getTimeZone("GMT"))
    val dateTimeStr = dfmT.format(cal.getTime)
    val dfmD = new SimpleDateFormat("yyyyMMdd")
    dfmD.setTimeZone(TimeZone.getTimeZone("GMT"))
    val dateStr = dfmD.format(cal.getTime)
    (dateTimeStr, dateStr)
  }

  val xAmzDate = "X-Amz-Date"

  private def signature(httpRequest: HttpRequest, dateTimeStr: String, dateStr: String): String = {
    val signature = hmacSHA256(stringToSign(httpRequest, dateTimeStr, dateStr), signingKey(dateStr))
    hexOf(signature)
  }

  private def signingKey(dateStr: String): Array[Byte] = {
    val kSecret = ("AWS4" + awsCredentials.getAWSSecretKey).getBytes("UTF8")
    val kDate = hmacSHA256(dateStr, kSecret)
    val kRegion = hmacSHA256(region, kDate)
    val kService = hmacSHA256(service, kRegion)
    hmacSHA256("aws4_request", kService)
  }


  private def credentialScope(dateStr: String) = {
    s"$dateStr/$region/$service/aws4_request"
  }

  private def canonicalRequestHash(httpRequest: HttpRequest) = {
    val canonicalRequest = createCanonicalRequest(httpRequest)
    hexOf(hashSha256(canonicalRequest.getBytes("UTF-8")))
  }


  private def canonicalUri(httpRequest: HttpRequest): String = {
    val path = httpRequest.uri.path.toString()
    val segments = path.split("/")
    segments.filter(_ != "").map(urlEncode).mkString(start = "/", sep = "/", end = "")
  }

  private def urlEncode(s: String): String = URLEncoder.encode(s, "utf-8")

  private def canonicalQueryString(httpRequest: HttpRequest): String = {
    val params = httpRequest.uri.query()
    val sortedEncoded = params.toList.map(kv => (urlEncode(kv._1), urlEncode(kv._2))).sortBy(_._1)
    sortedEncoded.map(kv => s"${kv._1}=${kv._2}").mkString("&")
  }

  private def canonicalHeaders(httpRequest: HttpRequest): String = {
    httpRequest.headers.sortBy(_.name.toLowerCase).map(header => s"${header.name.toLowerCase}:${header.value.trim}").mkString("\n") + "\n"
  }

  private def signedHeaders(httpRequest: HttpRequest): String = {
    httpRequest.headers.map(_.name.toLowerCase).sorted.mkString(";")
  }

  private def hashedPayloadByteArray(httpRequest: HttpRequest): Array[Byte] = {
    httpRequest.entity match {
      case HttpEntity.Strict(_, data) => hashSha256(data.toArray)
      case _ => throw new IllegalArgumentException("non strict http entity is not supported")
    }
  }

  private def hashSha256(data: Array[Byte]): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(data)
    val digest = md.digest()
    digest
  }


  private def hmacSHA256(data: String, key: Array[Byte]): Array[Byte] = {
    val algorithm = "HmacSHA256"
    val mac = Mac.getInstance(algorithm)
    mac.init(new SecretKeySpec(key, algorithm))
    mac.doFinal(data.getBytes("UTF8"))
  }

  private def hexOf(buf: Array[Byte]) = buf.map("%02X" format _).mkString.toLowerCase
}