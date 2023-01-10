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

package org.apache.kyuubi.ctl.cmd.list

import scala.collection.JavaConverters._

import org.apache.kyuubi.client.AdminRestApi
import org.apache.kyuubi.client.api.v1.dto.Engine
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ctl.util.Render

class ListEngineCommand(cliConfig: CliConfig) extends Command[Seq[Engine]](cliConfig) {

  override def validate(): Unit = {}

  def doRun(): Seq[Engine] = {
    withKyuubiRestClient(normalizedCliConfig, null, conf) { kyuubiRestClient =>
      val adminRestApi = new AdminRestApi(kyuubiRestClient)
      adminRestApi.listEngines(
        normalizedCliConfig.engineOpts.engineType,
        normalizedCliConfig.engineOpts.engineShareLevel,
        normalizedCliConfig.engineOpts.engineSubdomain,
        normalizedCliConfig.commonOpts.hs2ProxyUser).asScala
    }
  }

  def render(resp: Seq[Engine]): Unit = {
    info(Render.renderEngineNodesInfo(resp))
  }
}
