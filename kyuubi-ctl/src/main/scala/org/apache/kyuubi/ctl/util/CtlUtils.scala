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
package org.apache.kyuubi.ctl.util

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.{Map => JMap}

import org.yaml.snakeyaml.Yaml

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.ctl.opt.CliConfig

object CtlUtils {
  private[ctl] def loadYamlAsMap(cliConfig: CliConfig): JMap[String, Object] = {
    val filename = cliConfig.createOpts.filename

    var map: JMap[String, Object] = null
    var br: BufferedReader = null
    try {
      val yaml = new Yaml()
      val input = new FileInputStream(new File(filename))
      br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
      map = yaml.load(br).asInstanceOf[JMap[String, Object]]
    } catch {
      case e: Exception => throw new KyuubiException(s"Failed to read yaml file[$filename]: $e")
    } finally {
      if (br != null) {
        br.close()
      }
    }
    map
  }
}
