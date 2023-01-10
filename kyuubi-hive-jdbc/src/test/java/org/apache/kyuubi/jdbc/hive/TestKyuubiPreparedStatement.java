/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kyuubi.jdbc.hive;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestKyuubiPreparedStatement {

  @Mock private KyuubiConnection connection;
  @Mock private Iface client;
  @Mock private TSessionHandle sessHandle;
  @Mock TExecuteStatementResp tExecStatementResp;
  @Mock TGetOperationStatusResp tGetOperationStatusResp;
  private TStatus tStatus_SUCCESS = new TStatus(TStatusCode.SUCCESS_STATUS);
  @Mock private TOperationHandle tOperationHandle;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(tExecStatementResp.getStatus()).thenReturn(tStatus_SUCCESS);
    when(tExecStatementResp.getOperationHandle()).thenReturn(tOperationHandle);

    when(tGetOperationStatusResp.getStatus()).thenReturn(tStatus_SUCCESS);
    when(tGetOperationStatusResp.getOperationState()).thenReturn(TOperationState.FINISHED_STATE);
    when(tGetOperationStatusResp.isSetOperationState()).thenReturn(true);
    when(tGetOperationStatusResp.isSetOperationCompleted()).thenReturn(true);

    when(client.GetOperationStatus(any(TGetOperationStatusReq.class)))
        .thenReturn(tGetOperationStatusResp);
    when(client.ExecuteStatement(any(TExecuteStatementReq.class))).thenReturn(tExecStatementResp);
  }

  @Test
  public void testNonParameterized() throws Exception {
    String sql = "select 1";
    KyuubiPreparedStatement ps = new KyuubiPreparedStatement(connection, client, sessHandle, sql);
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1", argument.getValue().getStatement());
  }

  @Test
  public void unusedArgument() throws Exception {
    String sql = "select 1";
    KyuubiPreparedStatement ps = new KyuubiPreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "asd");
    ps.execute();
  }

  @Test(expected = SQLException.class)
  public void unsetArgument() throws Exception {
    String sql = "select 1 from x where a=?";
    KyuubiPreparedStatement ps = new KyuubiPreparedStatement(connection, client, sessHandle, sql);
    ps.execute();
  }

  @Test
  public void oneArgument() throws Exception {
    String sql = "select 1 from x where a=?";
    KyuubiPreparedStatement ps = new KyuubiPreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "asd");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='asd'", argument.getValue().getStatement());
  }

  @Test
  public void escapingOfStringArgument() throws Exception {
    String sql = "select 1 from x where a=?";
    KyuubiPreparedStatement ps = new KyuubiPreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "a'\"d");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='a\\'\"d'", argument.getValue().getStatement());
  }

  @Test
  public void pastingIntoQuery() throws Exception {
    String sql = "select 1 from x where a='e' || ?";
    KyuubiPreparedStatement ps = new KyuubiPreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "v");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='e' || 'v'", argument.getValue().getStatement());
  }

  // HIVE-13625
  @Test
  public void pastingIntoEscapedQuery() throws Exception {
    String sql = "select 1 from x where a='\\044e' || ?";
    KyuubiPreparedStatement ps = new KyuubiPreparedStatement(connection, client, sessHandle, sql);
    ps.setString(1, "v");
    ps.execute();

    ArgumentCaptor<TExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TExecuteStatementReq.class);
    verify(client).ExecuteStatement(argument.capture());
    assertEquals("select 1 from x where a='\\044e' || 'v'", argument.getValue().getStatement());
  }
}
