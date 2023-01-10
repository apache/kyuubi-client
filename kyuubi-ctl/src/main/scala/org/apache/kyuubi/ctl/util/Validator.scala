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

import java.nio.file.{Files, Paths}

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.ctl.opt.CliConfig

private[ctl] object Validator {
  def validateFilename(cliConfig: CliConfig): Unit = {
    val filename = cliConfig.createOpts.filename
    if (StringUtils.isBlank(filename)) {
      fail(s"Config file is not specified.")
    }

    if (!Files.exists(Paths.get(filename))) {
      fail(s"Config file does not exist: ${filename}.")
    }
  }

  def validateAdminConfigType(cliConfig: CliConfig): Unit = {
    if (cliConfig.adminConfigOpts.configType == null) {
      fail("The config type is not specified.")
    }
  }

  private def fail(msg: String): Unit = throw new KyuubiException(msg)
}
