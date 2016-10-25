/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.netty.example

import java.io.{BufferedReader, InputStreamReader}
import java.net._

import org.apache.commons.lang3.SystemUtils
import org.mortbay.util.MultiException
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Netty Utility class for start netty service and retry tcp port
 */
object NettyUtil {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /** find local inet addresses */
  def findLocalInetAddress(): InetAddress = {

    val address = InetAddress.getLocalHost
    address.isLoopbackAddress match {
      case true =>
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs = SystemUtils.IS_OS_WINDOWS match {
          case true => activeNetworkIFs
          case false => activeNetworkIFs.reverse
        }

        reOrderedNetworkIFs.find { ni: NetworkInterface =>
          val addr = ni.getInetAddresses.asScala.toSeq.filterNot { addr =>
            addr.isLinkLocalAddress || addr.isLoopbackAddress
          }
          addr.nonEmpty
        } match {
          case Some(ni) =>
            val addr = ni.getInetAddresses.asScala.toSeq.filterNot { inet =>
              inet.isLinkLocalAddress || inet.isLoopbackAddress
            }
            val address = addr.find(_.isInstanceOf[Inet4Address]).getOrElse(addr.head).getAddress
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            InetAddress.getByAddress(address)
          case None => address
        }
      case false => address
    }
  }

  /** start service, if port is collision, retry 128 times */
  def startServiceOnPort[T](
    startPort: Int,
    startService: Int => T,
    maxRetries: Int = 128,
    serviceName: String = ""): T = {

    if (startPort != 0 && (startPort < 1024 || startPort > 65536)) {
      throw new Exception("startPort should be between 1024 and 65535 (inclusive), " +
        "or 0 for a random free port.")
    }

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        // If the new port wraps around, do not try a privilege port
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }

      try {
        val result = startService(tryPort)
        logger.info(s"Successfully started service$serviceString, result:$result.")
        return result
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = s"${e.getMessage}: Service$serviceString failed after " +
              s"$maxRetries retries! Consider explicitly setting the appropriate port for the " +
              s"service$serviceString (for example spark.ui.port for SparkUI) to an available " +
              "port or increasing spark.port.maxRetries."
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logger.error(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new Exception(s"Failed to start service$serviceString on port $startPort")
  }

  /** send GET request to this url */
  def sendGetRequest(url: String): String = {
    val obj: URL = new URL(url)
    val con: HttpURLConnection = obj.openConnection.asInstanceOf[HttpURLConnection]
    con.setRequestMethod("GET")
    val code = try {
      con.getResponseCode
    } catch {
      case e: Throwable => e
    }

    code match {
      case HttpURLConnection.HTTP_OK =>
        val in: BufferedReader = new BufferedReader(new InputStreamReader(con.getInputStream))
        var inputLine: String = ""
        val response: StringBuilder = new StringBuilder
        try {
          while (inputLine != null) {
            response.append(inputLine)
            inputLine = in.readLine()
          }
          in.close()
        } catch {
          case throwable: Exception =>
        }
        response.toString
      case x => throw new Exception("GET request not worked of url: " + url)
    }
  }

  /** Return whether the exception is caused by an address-port collision when binding. */
  private def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException if e.getMessage != null => true
      case e: BindException => isBindCollision(e.getCause)
      case e: MultiException =>
        e.getThrowables.asScala.toList.map(_.asInstanceOf[Throwable]).exists(isBindCollision)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

}
