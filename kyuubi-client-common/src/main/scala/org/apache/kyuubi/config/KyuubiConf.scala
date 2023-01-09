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

package org.apache.kyuubi.config

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf._

case class KyuubiConf(loadSysDefault: Boolean = true) extends Logging {

  private val settings = new ConcurrentHashMap[String, String]()
  private lazy val reader: ConfigProvider = new ConfigProvider(settings)
  private def loadFromMap(props: Map[String, String]): Unit = {
    settings.putAll(props.asJava)
  }

  if (loadSysDefault) {
    val fromSysDefaults = Utils.getSystemProperties.filterKeys(_.startsWith("kyuubi."))
    loadFromMap(fromSysDefaults)
  }

  def loadFileDefaults(): KyuubiConf = {
    val maybeConfigFile = Utils.getDefaultPropertiesFile()
    loadFromMap(Utils.getPropertiesFromFile(maybeConfigFile))
    this
  }

  def set[T](entry: ConfigEntry[T], value: T): KyuubiConf = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(containsConfigEntry(entry), s"$entry is not registered")
    if (settings.put(entry.key, entry.strConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  def set[T](entry: OptionalConfigEntry[T], value: T): KyuubiConf = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    set(entry.key, entry.strConverter(Option(value)))
    this
  }

  def set(key: String, value: String): KyuubiConf = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    if (settings.put(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  def setIfMissing[T](entry: ConfigEntry[T], value: T): KyuubiConf = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    if (settings.putIfAbsent(entry.key, entry.strConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  def setIfMissing(key: String, value: String): KyuubiConf = {
    require(key != null)
    require(value != null)
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  def get[T](config: ConfigEntry[T]): T = {
    require(containsConfigEntry(config), s"$config is not registered")
    config.readFrom(reader)
  }

  def getOption(key: String): Option[String] = Option(settings.get(key))

  /** unset a parameter from the configuration */
  def unset(key: String): KyuubiConf = {
    logDeprecationWarning(key)
    settings.remove(key)
    this
  }

  def unset(entry: ConfigEntry[_]): KyuubiConf = {
    require(containsConfigEntry(entry), s"$entry is not registered")
    unset(entry.key)
  }

  /**
   * Get all parameters as map
   * sorted by key in ascending order
   */
  def getAll: Map[String, String] = {
    TreeMap(settings.asScala.toSeq: _*)
  }

  /** Get all envs as map */
  def getEnvs: Map[String, String] = {
    sys.env ++ getAllWithPrefix(KYUUBI_ENGINE_ENV_PREFIX, "")
  }

  /** Get all batch conf as map */
  def getBatchConf(batchType: String): Map[String, String] = {
    val normalizedBatchType = batchType.toLowerCase(Locale.ROOT) match {
      case "pyspark" => "spark"
      case other => other.toLowerCase(Locale.ROOT)
    }
    getAllWithPrefix(s"$KYUUBI_BATCH_CONF_PREFIX.$normalizedBatchType", "")
  }

  /**
   * Retrieve key-value pairs from [[KyuubiConf]] starting with `dropped.remainder`, and put them to
   * the result map with the `dropped` of key being dropped.
   * @param dropped first part of prefix which will dropped for the new key
   * @param remainder second part of the prefix which will be remained in the key
   */
  def getAllWithPrefix(dropped: String, remainder: String): Map[String, String] = {
    getAll.filter { case (k, _) => k.startsWith(s"$dropped.$remainder") }.map {
      case (k, v) => (k.substring(dropped.length + 1), v)
    }
  }

  /**
   * Retrieve user defaults configs in key-value pairs from [[KyuubiConf]] with key prefix "___"
   */
  def getAllUserDefaults: Map[String, String] = {
    getAll.filter { case (k, _) => k.startsWith(USER_DEFAULTS_CONF_QUOTE) }
  }

  /** Copy this object */
  override def clone: KyuubiConf = {
    val cloned = KyuubiConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey, e.getValue)
    }
    cloned
  }

  def getUserDefaults(user: String): KyuubiConf = {
    val cloned = KyuubiConf(false)

    for (e <- settings.entrySet().asScala if !e.getKey.startsWith(USER_DEFAULTS_CONF_QUOTE)) {
      cloned.set(e.getKey, e.getValue)
    }

    for ((k, v) <-
        getAllWithPrefix(s"$USER_DEFAULTS_CONF_QUOTE${user}$USER_DEFAULTS_CONF_QUOTE", "")) {
      cloned.set(k, v)
    }
    serverOnlyConfEntries.foreach(cloned.unset)
    cloned
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  private def logDeprecationWarning(key: String): Unit = {
    KyuubiConf.deprecatedConfigs.get(key).foreach {
      case DeprecatedConfig(configName, version, comment) =>
        logger.warn(
          s"The Kyuubi config '$configName' has been deprecated in Kyuubi v$version " +
            s"and may be removed in the future. $comment")
    }
  }
}

/**
 * Note to developers:
 * You need to rerun the test `org.apache.kyuubi.config.AllKyuubiConfiguration` locally if you
 * add or change a config. That can help to update the conf docs.
 */
object KyuubiConf {

  /** a custom directory that contains the [[KYUUBI_CONF_FILE_NAME]] */
  final val KYUUBI_CONF_DIR = "KYUUBI_CONF_DIR"

  /** the default file that contains kyuubi properties */
  final val KYUUBI_CONF_FILE_NAME = "kyuubi-defaults.conf"
  final val KYUUBI_HOME = "KYUUBI_HOME"
  final val KYUUBI_ENGINE_ENV_PREFIX = "kyuubi.engineEnv"
  final val KYUUBI_BATCH_CONF_PREFIX = "kyuubi.batchConf"
  final val USER_DEFAULTS_CONF_QUOTE = "___"

  private[this] val kyuubiConfEntriesUpdateLock = new Object

  @volatile
  private[this] var kyuubiConfEntries: java.util.Map[String, ConfigEntry[_]] =
    java.util.Collections.emptyMap()

  private var serverOnlyConfEntries: Set[ConfigEntry[_]] = Set()

  private[config] def register(entry: ConfigEntry[_]): Unit =
    kyuubiConfEntriesUpdateLock.synchronized {
      require(
        !kyuubiConfEntries.containsKey(entry.key),
        s"Duplicate ConfigEntry. ${entry.key} has been registered")
      val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](kyuubiConfEntries)
      updatedMap.put(entry.key, entry)
      kyuubiConfEntries = updatedMap
      if (entry.serverOnly) {
        serverOnlyConfEntries += entry
      }
    }

  // For testing only
  private[config] def unregister(entry: ConfigEntry[_]): Unit =
    kyuubiConfEntriesUpdateLock.synchronized {
      val updatedMap = new java.util.HashMap[String, ConfigEntry[_]](kyuubiConfEntries)
      updatedMap.remove(entry.key)
      kyuubiConfEntries = updatedMap
    }

  private[config] def getConfigEntry(key: String): ConfigEntry[_] = {
    kyuubiConfEntries.get(key)
  }

  private[config] def getConfigEntries(): java.util.Collection[ConfigEntry[_]] = {
    kyuubiConfEntries.values()
  }

  private[config] def containsConfigEntry(entry: ConfigEntry[_]): Boolean = {
    getConfigEntry(entry.key) == entry
  }

  def buildConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate(register)
  }

  /**
   * Holds information about keys that have been deprecated.
   *
   * @param key The deprecated key.
   * @param version Version of Kyuubi where key was deprecated.
   * @param comment Additional info regarding to the removed config. For example,
   *                reasons of config deprecation, what users should use instead of it.
   */
  case class DeprecatedConfig(key: String, version: String, comment: String)

  private val deprecatedConfigs: Map[String, DeprecatedConfig] = Map.empty
}
