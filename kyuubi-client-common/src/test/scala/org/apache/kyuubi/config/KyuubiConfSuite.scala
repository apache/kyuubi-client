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

import org.apache.kyuubi.KyuubiFunSuite

class KyuubiConfSuite extends KyuubiFunSuite {

  import KyuubiConf._

  test("kyuubi conf w/ w/o no sys defaults") {
    val key = "kyuubi.conf.abc"
    System.setProperty(key, "xyz")
    assert(KyuubiConf(false).getOption(key).isEmpty)
    assert(KyuubiConf(true).getAll.contains(key))
  }

  test("load default config file") {
    val conf = KyuubiConf().loadFileDefaults()
    assert(conf.getOption("kyuubi.yes").get === "yes")
    assert(conf.getOption("spark.kyuubi.yes").get === "no")
  }

  test("set and unset conf") {
    val conf = new KyuubiConf(false)

    val key = "kyuubi.conf.abc"
    conf.set(key, "opq")
    assert(conf.getOption(key) === Some("opq"))

    conf.unset(key)
    assert(conf.getOption(key).isEmpty)
  }

  test("clone") {
    val conf = KyuubiConf()
    val key = "kyuubi.abc.conf"
    conf.set(key, "xyz")
    val cloned = conf.clone
    assert(conf !== cloned)
    assert(cloned.getOption(key).get === "xyz")
  }

  test("get user specific defaults") {
    val conf = KyuubiConf().loadFileDefaults()

    assert(conf.getUserDefaults("kyuubi").getOption("spark.user.test").get === "a")
    assert(conf.getUserDefaults("userb").getOption("spark.user.test").get === "b")
    assert(conf.getUserDefaults("userc").getOption("spark.user.test").get === "c")
  }

  test("support arbitrary config from kyuubi-defaults") {
    val conf = KyuubiConf()
    assert(conf.getOption("user.name").isEmpty)
    conf.loadFileDefaults()
    assert(conf.getOption("abc").get === "xyz")
    assert(conf.getOption("xyz").get === "abc")
  }

  test("get pre-defined batch conf for different batch types") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(s"$KYUUBI_BATCH_CONF_PREFIX.spark.spark.yarn.tags", "kyuubi")
    kyuubiConf.set(s"$KYUUBI_BATCH_CONF_PREFIX.flink.yarn.tags", "kyuubi")
    assert(kyuubiConf.getBatchConf("spark") == Map("spark.yarn.tags" -> "kyuubi"))
    assert(kyuubiConf.getBatchConf("flink") == Map("yarn.tags" -> "kyuubi"))
  }
}
