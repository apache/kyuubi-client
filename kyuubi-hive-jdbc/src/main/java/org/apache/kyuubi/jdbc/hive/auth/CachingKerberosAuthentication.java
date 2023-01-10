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

package org.apache.kyuubi.jdbc.hive.auth;

import static java.util.Objects.requireNonNull;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

public class CachingKerberosAuthentication {
  private final KerberosAuthentication kerberosAuthentication;

  @GuardedBy("this")
  private Subject subject;

  @GuardedBy("this")
  private long nextRefreshTime;

  public CachingKerberosAuthentication(KerberosAuthentication kerberosAuthentication) {
    this.kerberosAuthentication =
        requireNonNull(kerberosAuthentication, "kerberosAuthentication is null");
  }

  public synchronized Subject getSubject() {
    if (subject == null || ticketNeedsRefresh()) {
      subject =
          requireNonNull(
              kerberosAuthentication.getSubject(), "kerberosAuthentication.getSubject() is null");
      KerberosTicket tgtTicket = KerberosUtils.getTgt(subject);
      nextRefreshTime = KerberosUtils.getTgtRefreshTime(tgtTicket);
    }
    return subject;
  }

  public synchronized void reauthenticateIfSoonWillBeExpired() {
    requireNonNull(subject, "subject is null, getSubject() must be called before reauthenticate()");
    if (ticketNeedsRefresh()) {
      kerberosAuthentication.attemptLogin(subject);
      KerberosTicket tgtTicket = KerberosUtils.getTgt(subject);
      nextRefreshTime = KerberosUtils.getTgtRefreshTime(tgtTicket);
    }
  }

  private boolean ticketNeedsRefresh() {
    return nextRefreshTime < System.currentTimeMillis();
  }
}
