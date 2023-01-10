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

package org.apache.kyuubi.ctl.cli

import scopt.OParser

import org.apache.kyuubi.ctl.opt.CliConfig

abstract private[kyuubi] class ControlCliArgumentsParser {

  /**
   * Cli arguments parse rules.
   */
  def parser(): OParser[Unit, CliConfig]

  /**
   * Parse a list of kyuubi-ctl command line options.
   *
   * @throws IllegalArgumentException If an error is found during parsing.
   */
  def parse(args: Seq[String]): Unit
}
