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
package org.apache.kyuubi.ctl.cmd

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.cli.ControlCli
import org.apache.kyuubi.ctl.opt.CliConfig

abstract class Command[T](cliConfig: CliConfig) extends Logging {

  val conf = KyuubiConf().loadFileDefaults()

  cliConfig.conf.foreach { case (key, value) =>
    conf.set(key, value)
  }

  val verbose = cliConfig.commonOpts.verbose

  val normalizedCliConfig: CliConfig = useDefaultPropertyValueIfMissing()

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  def validate(): Unit

  /** Run the command and return the internal result. */
  def doRun(): T

  /** Render the internal result. */
  def render(obj: T): Unit

  final def run(): Unit = {
    Option(doRun()).foreach(render)
  }

  def fail(msg: String): Unit = throw new KyuubiException(msg)

  protected def mergeArgsIntoKyuubiConf(): Unit = {}

  private def useDefaultPropertyValueIfMissing(): CliConfig = {
    cliConfig.copy()
  }

  override def info(msg: => Any): Unit = ControlCli.printMessage(msg)
  override def warn(msg: => Any): Unit = ControlCli.printMessage(s"Warning: $msg")
  override def error(msg: => Any): Unit = ControlCli.printMessage(s"Error: $msg")
}
