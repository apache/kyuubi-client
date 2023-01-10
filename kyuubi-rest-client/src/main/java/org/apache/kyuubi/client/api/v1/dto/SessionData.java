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

package org.apache.kyuubi.client.api.v1.dto;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SessionData {
  private String identifier;
  private String user;
  private String ipAddr;
  private Map<String, String> conf;
  private Long createTime;
  private Long duration;
  private Long idleTime;

  public SessionData() {}

  public SessionData(
      String identifier,
      String user,
      String ipAddr,
      Map<String, String> conf,
      Long createTime,
      Long duration,
      Long idleTime) {
    this.identifier = identifier;
    this.user = user;
    this.ipAddr = ipAddr;
    this.conf = conf;
    this.createTime = createTime;
    this.duration = duration;
    this.idleTime = idleTime;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public void setIpAddr(String ipAddr) {
    this.ipAddr = ipAddr;
  }

  public Map<String, String> getConf() {
    if (null == conf) {
      return Collections.emptyMap();
    }
    return conf;
  }

  public void setConf(Map<String, String> conf) {
    this.conf = conf;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public Long getDuration() {
    return duration;
  }

  public void setDuration(Long duration) {
    this.duration = duration;
  }

  public Long getIdleTime() {
    return idleTime;
  }

  public void setIdleTime(Long idleTime) {
    this.idleTime = idleTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SessionData that = (SessionData) o;
    return Objects.equals(getIdentifier(), that.getIdentifier());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getIdentifier());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
