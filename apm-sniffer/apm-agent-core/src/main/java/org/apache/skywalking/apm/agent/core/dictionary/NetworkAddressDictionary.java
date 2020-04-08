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
import org.apache.skywalking.apm.network.common.KeyIntValuePair;
import org.apache.skywalking.apm.network.register.v2.NetAddressMapping;
import org.apache.skywalking.apm.network.register.v2.NetAddresses;
import org.apache.skywalking.apm.network.register.v2.RegisterGrpc;

import static org.apache.skywalking.apm.agent.core.conf.Config.Dictionary.SERVICE_CODE_BUFFER_SIZE;

/**
 * Map of network address id to network literal address, which is from the collector side.
 * 维护了 地址与 appId的映射关系
 */
public enum NetworkAddressDictionary {
    INSTANCE;
    /**
     * key address  value  addressId  (addressId 相当于本条数据在 dao层的id)
     */
    private Map<String, Integer> serviceDictionary = new ConcurrentHashMap<>();
    private Set<String> unRegisterServices = ConcurrentHashMap.newKeySet();

    /**
     * 通过网络地址找到应用id
     * @param networkAddress
     * @return
     */
    public PossibleFound find(String networkAddress) {
        Integer applicationId = serviceDictionary.get(networkAddress);
        if (applicationId != null) {
            return new Found(applicationId);
        } else {
            // 当没有找到地址对应的app信息时 并不会选择立即注册 而是通过ServiceAndEnpointRegisterClient 进行批量注册
            if (serviceDictionary.size() + unRegisterServices.size() < SERVICE_CODE_BUFFER_SIZE) {
                unRegisterServices.add(networkAddress);
            }
            return new NotFound();
        }
    }

    /**
     * 从unRegister 容器中将数据转移到 serviceDictionary中
     * @param networkAddressRegisterServiceBlockingStub
     */
    public void syncRemoteDictionary(RegisterGrpc.RegisterBlockingStub networkAddressRegisterServiceBlockingStub) {
        if (unRegisterServices.size() > 0) {
            NetAddressMapping networkAddressMappings = networkAddressRegisterServiceBlockingStub
                .doNetworkAddressRegister(NetAddresses.newBuilder()
                                                      .addAllAddresses(unRegisterServices)
                                                      .build());
            // 这里应该是注册成功的id  也就是注册成功就会从容器中移除  第一次都不会立即注册成功 因为oap内部生产者与消费者是解耦的
            if (networkAddressMappings.getAddressIdsCount() > 0) {
                for (KeyIntValuePair keyWithIntegerValue : networkAddressMappings.getAddressIdsList()) {
                    unRegisterServices.remove(keyWithIntegerValue.getKey());
                    serviceDictionary.put(keyWithIntegerValue.getKey(), keyWithIntegerValue.getValue());
                }
            }
        }
    }

    /**
     * 清除当前维护的地址信息
     */
    public void clear() {
        this.serviceDictionary.clear();
    }
}
