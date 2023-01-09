/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.collection.JavaConverters._

object Utils extends Logging {

  import org.apache.kyuubi.config.KyuubiConf._

  def strToSeq(s: String, sp: String = ","): Seq[String] = {
    require(s != null)
    s.split(sp).map(_.trim).filter(_.nonEmpty)
  }

  def getSystemProperties: Map[String, String] = {
    sys.props.toMap
  }

  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): Option[File] = {
    getPropertiesFile(KYUUBI_CONF_FILE_NAME, env)
  }

  def getPropertiesFile(fileName: String, env: Map[String, String] = sys.env): Option[File] = {
    env.get(KYUUBI_CONF_DIR)
      .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
      .map(d => new File(d + File.separator + fileName))
      .filter(_.exists())
      .orElse {
        Option(Utils.getContextOrKyuubiClassLoader.getResource(fileName)).map { url =>
          new File(url.getFile)
        }.filter(_.exists())
      }
  }

  def getPropertiesFromFile(file: Option[File]): Map[String, String] = {
    file.map { f =>
      info(s"Loading Kyuubi properties from ${f.getAbsolutePath}")
      try {
        val reader = new InputStreamReader(f.toURI.toURL.openStream(), StandardCharsets.UTF_8)
        try {
          val properties = new Properties()
          properties.load(reader)
          properties.stringPropertyNames().asScala.map { k =>
            (k, properties.getProperty(k).trim)
          }.toMap
        } finally {
          reader.close()
        }
      } catch {
        case e: IOException =>
          throw new KyuubiException(
            s"Failed when loading Kyuubi properties from ${f.getAbsolutePath}",
            e)
      }
    }.getOrElse(Map.empty)
  }

  /**
   * Get the ClassLoader which loaded Kyuubi.
   */
  def getKyuubiClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Kyuubi.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrKyuubiClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getKyuubiClassLoader)
}
