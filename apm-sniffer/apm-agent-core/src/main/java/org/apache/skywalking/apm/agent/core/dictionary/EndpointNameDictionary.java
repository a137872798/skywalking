/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.dictionary;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.skywalking.apm.network.common.DetectPoint;
import org.apache.skywalking.apm.network.register.v2.Endpoint;
import org.apache.skywalking.apm.network.register.v2.EndpointMapping;
import org.apache.skywalking.apm.network.register.v2.EndpointMappingElement;
import org.apache.skywalking.apm.network.register.v2.Endpoints;
import org.apache.skywalking.apm.network.register.v2.RegisterGrpc;

import static org.apache.skywalking.apm.agent.core.conf.Config.Dictionary.ENDPOINT_NAME_BUFFER_SIZE;

public enum EndpointNameDictionary {
    INSTANCE;

    /**
     * key 包含了 endpoint 和 serviceId   value 对应operationId
     */
    private Map<OperationNameKey, Integer> endpointDictionary = new ConcurrentHashMap<>();
    private Set<OperationNameKey> unRegisterEndpoints = ConcurrentHashMap.newKeySet();

    /**
     * 当没有通过 endpoint找到serviceId 时 选择添加该映射关系
     * @param serviceId
     * @param endpointName
     * @return
     */
    public PossibleFound findOrPrepare4Register(int serviceId, String endpointName) {
        return find0(serviceId, endpointName, true);
    }

    public PossibleFound findOnly(int serviceId, String endpointName) {
        return find0(serviceId, endpointName, false);
    }

    /**
     * 通过endpoint 信息查找serviceId  如果没有找到 选择将该映射关系保存到容器中
     * @param serviceId
     * @param endpointName
     * @param registerWhenNotFound
     * @return
     */
    private PossibleFound find0(int serviceId, String endpointName, boolean registerWhenNotFound) {
        // 当传入无效的端点信息时 返回一个notFount
        if (endpointName == null || endpointName.length() == 0) {
            return new NotFound();
        }
        OperationNameKey key = new OperationNameKey(serviceId, endpointName);
        Integer operationId = endpointDictionary.get(key);
        if (operationId != null) {
            return new Found(operationId);
        } else {
            // 先选择添加到一个容器中 之后在一个定时任务中 通过批量发送来减少消耗的网络带宽
            if (registerWhenNotFound && endpointDictionary.size() + unRegisterEndpoints.size() < ENDPOINT_NAME_BUFFER_SIZE) {
                unRegisterEndpoints.add(key);
            }
            return new NotFound();
        }
    }

    public void syncRemoteDictionary(RegisterGrpc.RegisterBlockingStub serviceNameDiscoveryServiceBlockingStub) {
        if (unRegisterEndpoints.size() > 0) {
            // 抽取未注册的信息 并转换成 protobuf 的格式 看来是要访问某个服务器
            Endpoints.Builder builder = Endpoints.newBuilder();
            for (OperationNameKey operationNameKey : unRegisterEndpoints) {
                Endpoint endpoint = Endpoint.newBuilder()
                                            .setServiceId(operationNameKey.getServiceId())
                                            .setEndpointName(operationNameKey.getEndpointName())
                                            .setFrom(DetectPoint.server)
                                            .build();
                builder.addEndpoints(endpoint);
            }
            // 发起一个注册请求
            EndpointMapping serviceNameMappingCollection = serviceNameDiscoveryServiceBlockingStub.doEndpointRegister(builder
                .build());
            // 如果返回的结果中 包含这些数据 那么就可以成功转移
            if (serviceNameMappingCollection.getElementsCount() > 0) {
                for (EndpointMappingElement element : serviceNameMappingCollection.getElementsList()) {
                    OperationNameKey key = new OperationNameKey(element.getServiceId(), element.getEndpointName());
                    unRegisterEndpoints.remove(key);
                    endpointDictionary.put(key, element.getEndpointId());
                }
            }
        }
    }

    public void clear() {
        endpointDictionary.clear();
    }

    @Getter
    @ToString
    @RequiredArgsConstructor
    private static class OperationNameKey {
        private final int serviceId;
        private final String endpointName;

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            OperationNameKey key = (OperationNameKey) o;

            boolean isServiceEndpointMatch = false;
            if (serviceId == key.serviceId && endpointName.equals(key.endpointName)) {
                isServiceEndpointMatch = true;
            }
            return isServiceEndpointMatch;
        }

        @Override
        public int hashCode() {
            int result = serviceId;
            result = 31 * result + endpointName.hashCode();
            return result;
        }
    }
}
