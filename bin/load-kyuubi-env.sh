#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


export KYUUBI_HOME="${KYUUBI_HOME:-"$(cd "$(dirname "$0")"/.. || exit; pwd)"}"

export KYUUBI_CONF_DIR="${KYUUBI_CONF_DIR:-"${KYUUBI_HOME}"/conf}"

silent=0
while getopts "s" arg
do
  case $arg in
    s)
      silent=1
    ;;
    ?)
      echo "unknown argument"
      exit 1
    ;;
  esac
done

KYUUBI_ENV_SH="${KYUUBI_CONF_DIR}"/kyuubi-env.sh
if [[ -f ${KYUUBI_ENV_SH} ]]; then
  set -a
  if [ $silent -eq 0 ]; then
    echo "Using kyuubi environment file ${KYUUBI_ENV_SH} to initialize..."
  fi
  . "${KYUUBI_ENV_SH}"
  set +a
else
  echo "Warn: Not find kyuubi environment file ${KYUUBI_ENV_SH}, using default ones..."
fi

export KYUUBI_LOG_DIR="${KYUUBI_LOG_DIR:-"${KYUUBI_HOME}/logs"}"
if [[ ! -e ${KYUUBI_LOG_DIR} ]]; then
  mkdir -p ${KYUUBI_LOG_DIR}
fi

export KYUUBI_PID_DIR="${KYUUBI_PID_DIR:-"${KYUUBI_HOME}/pid"}"
if [[ ! -e ${KYUUBI_PID_DIR} ]]; then
  mkdir -p ${KYUUBI_PID_DIR}
fi

export KYUUBI_WORK_DIR_ROOT="${KYUUBI_WORK_DIR_ROOT:-"${KYUUBI_HOME}/work"}"
if [[ ! -e ${KYUUBI_WORK_DIR_ROOT} ]]; then
  mkdir -p "${KYUUBI_WORK_DIR_ROOT}"
fi

if [[ -z ${JAVA_HOME} ]]; then
  if [[ $(command -v java) ]]; then
    # shellcheck disable=SC2155
    export JAVA_HOME="$(dirname $(dirname $(which java)))"
  fi
fi

export KYUUBI_SCALA_VERSION="${KYUUBI_SCALA_VERSION:-"2.12"}"

# Print essential environment variables to console
if [ $silent -eq 0 ]; then
  echo "JAVA_HOME: ${JAVA_HOME}"

  echo "KYUUBI_HOME: ${KYUUBI_HOME}"
  echo "KYUUBI_CONF_DIR: ${KYUUBI_CONF_DIR}"
  echo "KYUUBI_LOG_DIR: ${KYUUBI_LOG_DIR}"
  echo "KYUUBI_PID_DIR: ${KYUUBI_PID_DIR}"
  echo "KYUUBI_WORK_DIR_ROOT: ${KYUUBI_WORK_DIR_ROOT}"

  echo "FLINK_HOME: ${FLINK_HOME}"
  echo "FLINK_ENGINE_HOME: ${FLINK_ENGINE_HOME}"

  echo "SPARK_HOME: ${SPARK_HOME}"
  echo "SPARK_CONF_DIR: ${SPARK_CONF_DIR:-"${SPARK_HOME}/conf"}"
  echo "SPARK_ENGINE_HOME: ${SPARK_ENGINE_HOME}"

  echo "TRINO_ENGINE_HOME: ${TRINO_ENGINE_HOME}"

  echo "HIVE_ENGINE_HOME: ${HIVE_ENGINE_HOME}"

  echo "HADOOP_CONF_DIR: ${HADOOP_CONF_DIR}"

  echo "YARN_CONF_DIR: ${YARN_CONF_DIR}"
fi
