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

package org.apache.kyuubi.client;

import java.util.HashMap;
import java.util.Map;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.CloseBatchResponse;
import org.apache.kyuubi.client.api.v1.dto.GetBatchesResponse;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.apache.kyuubi.client.util.JsonUtils;

public class BatchRestApi {

  private KyuubiRestClient client;

  private static final String API_BASE_PATH = "batches";

  private BatchRestApi() {}

  public BatchRestApi(KyuubiRestClient client) {
    this.client = client;
  }

  public Batch createBatch(BatchRequest request) {
    String requestBody = JsonUtils.toJson(request);
    return this.getClient().post(API_BASE_PATH, requestBody, Batch.class, client.getAuthHeader());
  }

  public Batch getBatchById(String batchId) {
    String path = String.format("%s/%s", API_BASE_PATH, batchId);
    return this.getClient().get(path, null, Batch.class, client.getAuthHeader());
  }

  public GetBatchesResponse listBatches(
      String batchType,
      String batchUser,
      String batchState,
      Long createTime,
      Long endTime,
      int from,
      int size) {
    Map<String, Object> params = new HashMap<>();
    params.put("batchType", batchType);
    params.put("batchUser", batchUser);
    params.put("batchState", batchState);
    if (null != createTime && createTime > 0) {
      params.put("createTime", createTime);
    }
    if (null != endTime && endTime > 0) {
      params.put("endTime", endTime);
    }
    params.put("from", from);
    params.put("size", size);
    return this.getClient()
        .get(API_BASE_PATH, params, GetBatchesResponse.class, client.getAuthHeader());
  }

  public OperationLog getBatchLocalLog(String batchId, int from, int size) {
    Map<String, Object> params = new HashMap<>();
    params.put("from", from);
    params.put("size", size);

    String path = String.format("%s/%s/localLog", API_BASE_PATH, batchId);
    return this.getClient().get(path, params, OperationLog.class, client.getAuthHeader());
  }

  public CloseBatchResponse deleteBatch(String batchId, String hs2ProxyUser) {
    Map<String, Object> params = new HashMap<>();
    params.put("hive.server2.proxy.user", hs2ProxyUser);

    String path = String.format("%s/%s", API_BASE_PATH, batchId);
    return this.getClient().delete(path, params, CloseBatchResponse.class, client.getAuthHeader());
  }

  private IRestClient getClient() {
    return this.client.getHttpClient();
  }
}
