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

package org.apache.kyuubi.ctl

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite}
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cli.ControlCliArguments

class ControlCliArgumentsSuite extends KyuubiFunSuite with TestPrematureExit {
  val zkQuorum = "localhost:2181"
  val namespace = "kyuubi"
  val user = "kyuubi"
  val host = "localhost"
  val port = "10000"

  /** Check whether the script exits and the given search string is printed. */
  private def testHelpExit(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run(): Unit =
          try {
            new ControlCliArguments(args) {
              override private[kyuubi] lazy val effectSetup = new KyuubiOEffectSetup {
                // nothing to do, to handle out stream.
                override def terminate(exitState: Either[String, Unit]): Unit = ()
              }
            }
          } catch {
            case e: Exception =>
              error(e)
          }
      }
      thread.start()
      thread.join()
      assert(logAppender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.contains(searchString)))
    }
  }

  test("prints usage on empty input") {
    testPrematureExitForControlCliArgs(
      Array.empty[String],
      "Must specify action command: [create|get|delete|list|log|submit].")
    testPrematureExitForControlCliArgs(
      Array("--verbose"),
      "Must specify action command: [create|get|delete|list|log|submit].")
  }

  test("prints error with unrecognized options") {
    testPrematureExitForControlCliArgs(Array("create", "--unknown"), "Unknown option --unknown")
    testPrematureExitForControlCliArgs(Array("--unknown"), "Unknown option --unknown")
  }

  test("test invalid arguments") {
    // for server, user option is not support
    testPrematureExitForControlCliArgs(Array("create", "--user"), "Unknown option --user")
    // for engine, user option need a value
    testPrematureExitForControlCliArgs(
      Array("get", "engine", "--user"),
      "Missing value after --user")
  }

  test("test extra unused arguments") {
    val args = Array(
      "list",
      "extraArg1",
      "extraArg2")
    testPrematureExitForControlCliArgs(args, "Unknown argument 'extraArg1'")
  }

  test("test --help") {
    // some string is too long for check style
    val waitBatchCompletionHelpString = "Boolean property. If true(default), the client process " +
      "will stay alive until the batch is in any terminal state. If false, the client will exit " +
      "when the batch is no longer in PENDING state."
    val helpString =
      s"""kyuubi $KYUUBI_VERSION
         |Usage: kyuubi-ctl [create|get|delete|list|log|submit] [options]
         |
         |  -b, --verbose            Print additional debug output.
         |  --hostUrl <value>        Host url for rest api.
         |  --authSchema <value>     Auth schema for rest api, valid values are basic, spnego.
         |  --username <value>       Username for basic authentication.
         |  --password <value>       Password for basic authentication.
         |  --spnegoHost <value>     Spnego host for spnego authentication.
         |  --hs2ProxyUser <value>   The value of hive.server2.proxy.user config.
         |  --conf <value>           Kyuubi config property pair, formatted key=value.
         |
         |Command: create [batch] [options]
         |${"\t"}Create a resource.
         |  -f, --filename <value>   Filename to use to create the resource
         |Command: create batch
         |${"\t"}Open batch session.
         |
         |Command: get [batch|engine] <args>...
         |${"\t"}Display information about the specified resources.
         |Command: get batch [<batchId>]
         |${"\t"}Get batch by id.
         |  <batchId>                Batch id.
         |Command: get engine [options]
         |${"\t"}Get Kyuubi engine info belong to a user.
         |  -u, --user <value>       The user name this engine belong to.
         |  -et, --engine-type <value>
         |                           The engine type this engine belong to.
         |  -es, --engine-subdomain <value>
         |                           The engine subdomain this engine belong to.
         |  -esl, --engine-share-level <value>
         |                           The engine share level this engine belong to.
         |
         |Command: delete [batch|engine] <args>...
         |${"\t"}Delete resources.
         |Command: delete batch [<batchId>]
         |${"\t"}Close batch session.
         |  <batchId>                Batch id.
         |Command: delete engine [options]
         |${"\t"}Delete the specified engine node for user.
         |  -u, --user <value>       The user name this engine belong to.
         |  -et, --engine-type <value>
         |                           The engine type this engine belong to.
         |  -es, --engine-subdomain <value>
         |                           The engine subdomain this engine belong to.
         |  -esl, --engine-share-level <value>
         |                           The engine share level this engine belong to.
         |
         |Command: list [batch|session|engine]
         |${"\t"}List information about resources.
         |Command: list batch [options]
         |${"\t"}List batch session info.
         |  --batchType <value>      Batch type.
         |  --batchUser <value>      Batch user.
         |  --batchState <value>     Batch state.
         |  --createTime <value>     Batch create time, should be in yyyyMMddHHmmss format.
         |  --endTime <value>        Batch end time, should be in yyyyMMddHHmmss format.
         |  --from <value>           Specify which record to start from retrieving info.
         |  --size <value>           The max number of records returned in the query.
         |Command: list session
         |${"\t"}List all the live sessions
         |Command: list engine [options]
         |${"\t"}List all the engine nodes for a user
         |  -u, --user <value>       The user name this engine belong to.
         |  -et, --engine-type <value>
         |                           The engine type this engine belong to.
         |  -es, --engine-subdomain <value>
         |                           The engine subdomain this engine belong to.
         |  -esl, --engine-share-level <value>
         |                           The engine share level this engine belong to.
         |
         |Command: log [batch] [options] <args>...
         |${"\t"}Print the logs for specified resource.
         |  --forward                If forward is specified, the ctl will block forever.
         |Command: log batch [options] [<batchId>]
         |${"\t"}Get batch session local log.
         |  <batchId>                Batch id.
         |  --from <value>           Specify which record to start from retrieving info.
         |  --size <value>           The max number of records returned in the query.
         |
         |Command: submit [batch] [options]
         |${"\t"}Combination of create, get and log commands.
         |  -f, --filename <value>   Filename to use to create the resource
         |Command: submit batch [options]
         |${"\t"}open batch session and wait for completion.
         |  --waitCompletion <value>
         |                           ${waitBatchCompletionHelpString}
         |
         |  -h, --help               Show help message and exit.""".stripMargin

    testHelpExit(Array("--help"), helpString)
  }

  test("test kyuubi conf property") {
    val args = Seq(
      "delete",
      "batch",
      "123",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key}=10")
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliConfig.action.toString.equalsIgnoreCase("DELETE"))
    assert(opArgs.cliConfig.resource.toString.equalsIgnoreCase("BATCH"))
    assert(opArgs.cliConfig.batchOpts.batchId === "123")
    assert(opArgs.cliConfig.conf ===
      Map(CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key -> "10"))

    val args2 = Seq(
      "delete",
      "batch",
      "123",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key}")
    testPrematureExitForControlCliArgs(args2.toArray, "Kyuubi config without '='")
  }

  test("test ctl client conf") {
    val args = Seq(
      "delete",
      "batch",
      "123",
      "--hostUrl",
      "https://kyuubi.test.com",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_CONNECT_TIMEOUT.key}=5000",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key}=1")
    val opArgs = new ControlCliArguments(args)
    withKyuubiRestClient(opArgs.cliConfig, null, opArgs.command.conf) { kyuubiRestClient =>
      assert(kyuubiRestClient.getConf.getConnectTimeout == 5000)
      assert(kyuubiRestClient.getConf.getSocketTimeout == 120000)
      assert(kyuubiRestClient.getConf.getMaxAttempts == 1)
      assert(kyuubiRestClient.getConf.getAttemptWaitTime == 3000)
    }
  }
}
