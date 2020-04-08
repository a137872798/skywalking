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

package org.apache.skywalking.apm.agent.core.remote;

import io.grpc.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.commands.CommandService;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.conf.RemoteDownstreamConfig;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;
import org.apache.skywalking.apm.agent.core.dictionary.EndpointNameDictionary;
import org.apache.skywalking.apm.agent.core.dictionary.NetworkAddressDictionary;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.os.OSUtil;
import org.apache.skywalking.apm.network.common.Commands;
import org.apache.skywalking.apm.network.common.KeyIntValuePair;
import org.apache.skywalking.apm.network.common.KeyStringValuePair;
import org.apache.skywalking.apm.network.common.ServiceType;
import org.apache.skywalking.apm.network.register.v2.RegisterGrpc;
import org.apache.skywalking.apm.network.register.v2.Service;
import org.apache.skywalking.apm.network.register.v2.ServiceInstance;
import org.apache.skywalking.apm.network.register.v2.ServiceInstancePingGrpc;
import org.apache.skywalking.apm.network.register.v2.ServiceInstancePingPkg;
import org.apache.skywalking.apm.network.register.v2.ServiceInstanceRegisterMapping;
import org.apache.skywalking.apm.network.register.v2.ServiceInstances;
import org.apache.skywalking.apm.network.register.v2.ServiceRegisterMapping;
import org.apache.skywalking.apm.network.register.v2.Services;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.apm.util.StringUtil;

import static org.apache.skywalking.apm.agent.core.conf.Config.Collector.GRPC_UPSTREAM_TIMEOUT;

/**
 * 该对象将本服务实例信息注册到 oap 上   每一个植入了探针的应用被称为 Agent
 */
@DefaultImplementor
public class ServiceAndEndpointRegisterClient implements BootService, Runnable, GRPCChannelListener {
    private static final ILog logger = LogManager.getLogger(ServiceAndEndpointRegisterClient.class);
    /**
     * 服务实例id  通过 UUID 算法生成
     */
    private static String INSTANCE_UUID;

    /**
     * 一组键值对   用于描述服务器的信息  这里的数据应该是要发送到 oap 注册用的
     */
    private static List<KeyStringValuePair> SERVICE_INSTANCE_PROPERTIES;

    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;
    private volatile RegisterGrpc.RegisterBlockingStub registerBlockingStub;
    private volatile ServiceInstancePingGrpc.ServiceInstancePingBlockingStub serviceInstancePingStub;
    private volatile ScheduledFuture<?> applicationRegisterFuture;
    /**
     * 代表一个冷停止 时间戳 当需要重置 service 时会设置该标识
     */
    private volatile long coolDownStartTime = -1;

    /**
     * 监听状态的变化 创建存根对象
     *
     * @param status
     */
    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            // 通过连接对象来创建存根信息  存根对象就是用来发起调用的  就类似于 dubbo.consumer 暴露出来的api
            registerBlockingStub = RegisterGrpc.newBlockingStub(channel);
            serviceInstancePingStub = ServiceInstancePingGrpc.newBlockingStub(channel);
        } else {
            // 如果检测到连接断开了 那么将存根对象置空
            registerBlockingStub = null;
            serviceInstancePingStub = null;
        }
        this.status = status;
    }

    /**
     * 做一些前置准备
     */
    @Override
    public void prepare() {
        // 看来一个 引入了 skywalking的应用 只会对应一个 GRPCChannelManager
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);

        // 获取本服务实例id
        INSTANCE_UUID = StringUtil.isEmpty(Config.Agent.INSTANCE_UUID)
                ? UUID.randomUUID().toString().replaceAll("-", "")
                : Config.Agent.INSTANCE_UUID;

        SERVICE_INSTANCE_PROPERTIES = new ArrayList<>();

        // 将从配置文件中读取出来的配置 转移到该对象
        for (String key : Config.Agent.INSTANCE_PROPERTIES.keySet()) {
            SERVICE_INSTANCE_PROPERTIES.add(KeyStringValuePair.newBuilder()
                    .setKey(key)
                    .setValue(Config.Agent.INSTANCE_PROPERTIES.get(key))
                    .build());
        }
    }

    /**
     * 定时将自身信息注册到后端服务器
     */
    @Override
    public void boot() {
        applicationRegisterFuture = Executors.newSingleThreadScheduledExecutor(
                new DefaultNamedThreadFactory("ServiceAndEndpointRegisterClient")
        ).scheduleAtFixedRate(
                new RunnableWithExceptionProtection(
                        this,
                        t -> logger.error("unexpected exception.", t)
                ), 0, Config.Collector.APP_AND_SERVICE_REGISTER_CHECK_INTERVAL,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void onComplete() {
    }

    @Override
    public void shutdown() {
        applicationRegisterFuture.cancel(true);
    }

    /**
     * 将自身信息注册到 oap
     */
    @Override
    public void run() {
        logger.debug("ServiceAndEndpointRegisterClient running, status:{}.", status);

        // 当触发了 coolDown 时 代表短期内当前服务id 不可靠(比如即将被重置)
        if (coolDownStartTime > 0) {
            final long coolDownDurationInMillis = TimeUnit.MINUTES.toMillis(Config.Agent.COOL_DOWN_THRESHOLD);
            if (System.currentTimeMillis() - coolDownStartTime < coolDownDurationInMillis) {
                // 代表暂时不可用 等待下次循环
                logger.warn("The agent is cooling down, won't register itself");
                return;
            } else {
                logger.warn("The agent is re-registering itself to backend");
            }
        }
        coolDownStartTime = -1;

        boolean shouldTry = true;
        // 在已经连接到服务器的基础上 准备发送数据
        while (GRPCChannelStatus.CONNECTED.equals(status) && shouldTry) {
            shouldTry = false;
            try {
                // 当前服务id 还没有初始化时 才需要注册  也就是服务id 是在oap实现的
                if (RemoteDownstreamConfig.Agent.SERVICE_ID == DictionaryUtil.nullValue()) {
                    if (registerBlockingStub != null) {
                        ServiceRegisterMapping serviceRegisterMapping = registerBlockingStub.withDeadlineAfter(
                                GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                        ).doServiceRegister(
                                Services.newBuilder()
                                        .addServices(
                                                Service
                                                        .newBuilder()
                                                        // 从配置中获取服务名称
                                                        .setServiceName(Config.Agent.SERVICE_NAME)
                                                        // 这样代表是一个普通应用  如果是 DB 之类在skywalking的架构中 也被看作是一个 service
                                                        .setType(ServiceType.normal))
                                        .build()
                        );
                        // 返回结果
                        if (serviceRegisterMapping != null) {
                            // 之后尝试注册服务实例
                            for (KeyIntValuePair registered : serviceRegisterMapping.getServicesList()) {
                                if (Config.Agent.SERVICE_NAME.equals(registered.getKey())) {
                                    RemoteDownstreamConfig.Agent.SERVICE_ID = registered.getValue();
                                    // 为了进入下次循环
                                    shouldTry = true;
                                }
                            }
                        }
                    }
                } else {
                    if (registerBlockingStub != null) {
                        // 根据生成的服务id 来填充服务实例 并注册到 oap 上  在oap集群中会尽可能的将数据集中到某个节点上
                        if (RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID == DictionaryUtil.nullValue()) {

                            ServiceInstanceRegisterMapping instanceMapping = registerBlockingStub.withDeadlineAfter(
                                    GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                            ).doServiceInstanceRegister(
                                    ServiceInstances
                                            .newBuilder()
                                            .addInstances(
                                                    ServiceInstance
                                                            .newBuilder()
                                                            .setServiceId(RemoteDownstreamConfig.Agent.SERVICE_ID)
                                                            .setInstanceUUID(INSTANCE_UUID)
                                                            .setTime(System.currentTimeMillis())
                                                            .addAllProperties(OSUtil.buildOSInfo())
                                                            .addAllProperties(SERVICE_INSTANCE_PROPERTIES))
                                            .build());
                            // 从结果中获取所有服务实例信息
                            for (KeyIntValuePair serviceInstance : instanceMapping.getServiceInstancesList()) {
                                if (INSTANCE_UUID.equals(serviceInstance.getKey())) {
                                    // 更新当前节点的服务实例信息
                                    int serviceInstanceId = serviceInstance.getValue();
                                    if (serviceInstanceId != DictionaryUtil.nullValue()) {
                                        RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID = serviceInstanceId;
                                        RemoteDownstreamConfig.Agent.INSTANCE_REGISTERED_TIME = System.currentTimeMillis();
                                    }
                                }
                            }
                        // 这是一个定时任务 也就是会定期往 oap 发送心跳信息
                        } else {
                            final Commands commands = serviceInstancePingStub.withDeadlineAfter(
                                    GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                            ).doPing(ServiceInstancePingPkg.newBuilder()
                                    .setServiceInstanceId(
                                            RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID)
                                    .setTime(System.currentTimeMillis())
                                    .setServiceInstanceUUID(INSTANCE_UUID)
                                    .build());

                            // 首次启动时 这2个容器都为空 每次执行某些动作 将数据存放到待发送的容器后 在这个定时任务中将数据注册到 oap 上
                            // 难怪有些注册数据不是直接发送到 oap 而是在这个时机进行批量发送 借此提高性能
                            NetworkAddressDictionary.INSTANCE.syncRemoteDictionary(
                                    registerBlockingStub.withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS));
                            // 同样注册端点信息
                            EndpointNameDictionary.INSTANCE.syncRemoteDictionary(
                                    registerBlockingStub.withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS));
                            // 处理从 oap 返回的command  比如在发送心跳尝试更新某个服务数据时 发现服务已经找不到了 那么就要重新发起请i去 (ResetCommand)
                            // 是为了不让定时线程被其他无关的事物占用么 所以将command 保存后 由专门的线程来处理
                            ServiceManager.INSTANCE.findService(CommandService.class).receiveCommand(commands);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error(t, "ServiceAndEndpointRegisterClient execute fail.");
                // 设置 GRPCChannelManager 的 reconnect 标识 同时触发监听器
                ServiceManager.INSTANCE.findService(GRPCChannelManager.class).reportError(t);
            }
        }
    }

    public void coolDown() {
        this.coolDownStartTime = System.currentTimeMillis();
    }
}
