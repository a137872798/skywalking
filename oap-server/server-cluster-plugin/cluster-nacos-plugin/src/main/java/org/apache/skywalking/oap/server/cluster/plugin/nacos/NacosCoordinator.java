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

package org.apache.skywalking.oap.server.cluster.plugin.nacos;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import java.util.ArrayList;
import java.util.List;
import org.apache.skywalking.oap.server.core.cluster.ClusterNodesQuery;
import org.apache.skywalking.oap.server.core.cluster.ClusterRegister;
import org.apache.skywalking.oap.server.core.cluster.RemoteInstance;
import org.apache.skywalking.oap.server.core.cluster.ServiceQueryException;
import org.apache.skywalking.oap.server.core.cluster.ServiceRegisterException;
import org.apache.skywalking.oap.server.core.remote.client.Address;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.telemetry.api.TelemetryRelatedContext;

/**
 * 通过该对象可以拉取整个集群范围内所有的节点
 */
public class NacosCoordinator implements ClusterRegister, ClusterNodesQuery {

    private final NamingService namingService;
    private final ClusterModuleNacosConfig config;

    /**
     * 本机地址
     */
    private volatile Address selfAddress;

    public NacosCoordinator(NamingService namingService, ClusterModuleNacosConfig config) {
        this.namingService = namingService;
        this.config = config;
    }

    /**
     * 开始拉取集群范围所有节点
     * @return
     */
    @Override
    public List<RemoteInstance> queryRemoteNodes() {
        List<RemoteInstance> result = new ArrayList<>();
        try {
            // config.serviceName 就代表集群的名字  这样理解 配置中心基于 raft 协议 那么在单独的配置中心修改集群下节点时 确保在全局范围内一致
            // 然后连接任意一个配置中心节点 直接获取配置就好 (集群中包含哪些节点就是从配置文件读取) 那么下面的操作就可以简洁的看作从配置文件中读取某集群下所有节点
            List<Instance> instances = namingService.selectInstances(config.getServiceName(), true);
            if (CollectionUtils.isNotEmpty(instances)) {
                instances.forEach(instance -> {
                    Address address = new Address(instance.getIp(), instance.getPort(), false);
                    if (address.equals(selfAddress)) {
                        address.setSelf(true);
                    }
                    result.add(new RemoteInstance(address));
                });
            }
        } catch (NacosException e) {
            throw new ServiceQueryException(e.getErrMsg());
        }
        return result;
    }

    /**
     * 向配置中心注册配置
     * @param remoteInstance
     * @throws ServiceRegisterException
     */
    @Override
    public void registerRemote(RemoteInstance remoteInstance) throws ServiceRegisterException {
        String host = remoteInstance.getAddress().getHost();
        int port = remoteInstance.getAddress().getPort();
        try {
            namingService.registerInstance(config.getServiceName(), host, port);
        } catch (Exception e) {
            throw new ServiceRegisterException(e.getMessage());
        }
        this.selfAddress = remoteInstance.getAddress();
        TelemetryRelatedContext.INSTANCE.setId(selfAddress.toString());
    }
}
