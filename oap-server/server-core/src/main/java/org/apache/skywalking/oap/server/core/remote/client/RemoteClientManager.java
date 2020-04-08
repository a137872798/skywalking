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

package org.apache.skywalking.oap.server.core.remote.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.oap.server.core.cluster.ClusterModule;
import org.apache.skywalking.oap.server.core.cluster.ClusterNodesQuery;
import org.apache.skywalking.oap.server.core.cluster.RemoteInstance;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.apache.skywalking.oap.server.library.module.Service;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.GaugeMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the connections between OAP servers. There is a task schedule that will automatically query a
 * server list from the cluster module. Such as Zookeeper cluster module or Kubernetes cluster module.
 * 该对象用于管理所有客户端 每个客户端连接到不同的server
 */
public class RemoteClientManager implements Service {

    private static final Logger logger = LoggerFactory.getLogger(RemoteClientManager.class);

    private final ModuleDefineHolder moduleDefineHolder;
    /**
     * 该对象负责查询当前集群有哪些节点
     */
    private ClusterNodesQuery clusterNodesQuery;
    /**
     * 连接到集群 其他节点的client
     */
    private volatile List<RemoteClient> usingClients;
    /**
     * 有关使用率的 测量数据  该对象相当于一个适配器 底层是第三方的统计类
     */
    private GaugeMetrics gauge;
    /**
     * 连接到远端的超时时间
     */
    private int remoteTimeout;

    /**
     * Initial the manager for all remote communication clients.
     *
     * @param moduleDefineHolder for looking up other modules
     * @param remoteTimeout      for cluster internal communication, in second unit.
     */
    public RemoteClientManager(ModuleDefineHolder moduleDefineHolder, int remoteTimeout) {
        this.moduleDefineHolder = moduleDefineHolder;
        this.usingClients = ImmutableList.of();
        this.remoteTimeout = remoteTimeout;
    }

    /**
     * 启动该服务时 从配置中心拉取最新的集群信息 并建立连接
     */
    public void start() {
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(this::refresh, 1, 5, TimeUnit.SECONDS);
    }

    /**
     * Query OAP server list from the cluster module and create a new connection for the new node. Make the OAP server
     * orderly because of each of the server will send stream data to each other by hash code.
     */
    void refresh() {
        // 通过遥感模块 创建统计数据
        if (gauge == null) {
            gauge = moduleDefineHolder.find(TelemetryModule.NAME)
                                      .provider()
                                      .getService(MetricsCreator.class)
                                      .createGauge("cluster_size", "Cluster size of current oap node", MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);
        }
        try {
            if (Objects.isNull(clusterNodesQuery)) {
                synchronized (RemoteClientManager.class) {
                    if (Objects.isNull(clusterNodesQuery)) {
                        this.clusterNodesQuery = moduleDefineHolder.find(ClusterModule.NAME)
                                                                   .provider()
                                                                   .getService(ClusterNodesQuery.class);
                    }
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Refresh remote nodes collection.");
            }

            // 拉取集群内其他节点信息  也就是没有中心的服务器 而是将数据同步到其他节点
            List<RemoteInstance> instanceList = clusterNodesQuery.queryRemoteNodes();
            instanceList = distinct(instanceList);
            Collections.sort(instanceList);

            // 更新当前统计数据  推测还会跟时间挂钩 比如啥时候更新的数据 这样才好展示一个完整的图表
            gauge.setValue(instanceList.size());

            if (logger.isDebugEnabled()) {
                instanceList.forEach(instance -> logger.debug("Cluster instance: {}", instance.toString()));
            }

            // 判断 集群内节点是否发生了变化
            if (!compare(instanceList)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("ReBuilding remote clients.");
                }
                // 发生变化时 重建通往其他节点的连接
                reBuildRemoteClients(instanceList);
            }

            // 打印当前客户端信息
            printRemoteClientList();
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    /**
     * Print the client list into log for confirm how many clients built.
     */
    private void printRemoteClientList() {
        if (logger.isDebugEnabled()) {
            StringBuilder addresses = new StringBuilder();
            this.usingClients.forEach(client -> addresses.append(client.getAddress().toString()).append(","));
            logger.debug("Remote client list: {}", addresses);
        }
    }

    /**
     * Because of OAP server register by the UUID which one-to-one mapping with process number. The register information
     * not delete immediately after process shutdown because of there is always happened network fault, not really
     * process shutdown. So, cluster module must wait a few seconds to confirm it. Then there are more than one register
     * information in the cluster.
     *
     * @param instanceList the instances query from cluster module.
     * @return distinct remote instances’
     * 去重
     */
    private List<RemoteInstance> distinct(List<RemoteInstance> instanceList) {
        Set<Address> addresses = new HashSet<>();
        List<RemoteInstance> newInstanceList = new ArrayList<>();
        instanceList.forEach(instance -> {
            if (addresses.add(instance.getAddress())) {
                newInstanceList.add(instance);
            }
        });
        return newInstanceList;
    }

    public List<RemoteClient> getRemoteClient() {
        return usingClients;
    }

    /**
     * Compare clients between exist clients and remote instance collection. Move the clients into new client collection
     * which are alive to avoid create a new channel. Shutdown the clients which could not find in cluster config.
     * <p>
     * Create a gRPC client for remote instance except for self-instance.
     *
     * @param remoteInstances Remote instance collection by query cluster config.
     *                        重建client连接
     */
    private void reBuildRemoteClients(List<RemoteInstance> remoteInstances) {
        final Map<Address, RemoteClientAction> remoteClientCollection = this.usingClients.stream()
                                                                                         .collect(Collectors.toMap(RemoteClient::getAddress,
                                                                                                 // 将每个client 对象与一个关闭动作封装在一起
                                                                                                 client -> new RemoteClientAction(client, Action.Close)));

        // 生成一组 创建动作
        final Map<Address, RemoteClientAction> latestRemoteClients = remoteInstances.stream()
                                                                                    .collect(Collectors.toMap(RemoteInstance::getAddress, remote -> new RemoteClientAction(null, Action.Create)));

        // 获取交集
        final Set<Address> unChangeAddresses = Sets.intersection(remoteClientCollection.keySet(), latestRemoteClients.keySet());

        unChangeAddresses.stream()
                         .filter(remoteClientCollection::containsKey)
                            // 更新成 unchanged 动作
                         .forEach(unChangeAddress -> remoteClientCollection.get(unChangeAddress)
                                                                           .setAction(Action.Unchanged));

        // make the latestRemoteClients including the new clients only
        // 移除掉之前已经存在的
        unChangeAddresses.forEach(latestRemoteClients::remove);
        remoteClientCollection.putAll(latestRemoteClients);

        final List<RemoteClient> newRemoteClients = new LinkedList<>();
        remoteClientCollection.forEach((address, clientAction) -> {
            switch (clientAction.getAction()) {
                case Unchanged:
                    newRemoteClients.add(clientAction.getRemoteClient());
                    break;
                case Create:
                    // 如果该节点就是自身 创建一个虚假的client
                    if (address.isSelf()) {
                        RemoteClient client = new SelfRemoteClient(moduleDefineHolder, address);
                        newRemoteClients.add(client);
                    // 生成连接到对端的client   当该对象接收到消息时 会自动发送到远端
                    } else {
                        RemoteClient client = new GRPCRemoteClient(moduleDefineHolder, address, 1, 3000, remoteTimeout);
                        client.connect();
                        newRemoteClients.add(client);
                    }
                    break;
            }
        });

        //for stable ordering for rolling selector
        Collections.sort(newRemoteClients);
        this.usingClients = ImmutableList.copyOf(newRemoteClients);

        remoteClientCollection.values()
                              .stream()
                              .filter(remoteClientAction -> remoteClientAction.getAction().equals(Action.Close))
                                // 其余client 关闭
                              .forEach(remoteClientAction -> remoteClientAction.getRemoteClient().close());
    }

    private boolean compare(List<RemoteInstance> remoteInstances) {
        if (usingClients.size() == remoteInstances.size()) {
            for (int i = 0; i < usingClients.size(); i++) {
                if (!usingClients.get(i).getAddress().equals(remoteInstances.get(i).getAddress())) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    enum Action {
        Close, Unchanged, Create
    }

    @Getter
    @AllArgsConstructor
    static private class RemoteClientAction {
        private RemoteClient remoteClient;

        @Setter
        private Action action;
    }
}
