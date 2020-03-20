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
 * 相当于是一个与服务器通信的总控制器
 */
@DefaultImplementor
public class ServiceAndEndpointRegisterClient implements BootService, Runnable, GRPCChannelListener {
    private static final ILog logger = LogManager.getLogger(ServiceAndEndpointRegisterClient.class);
    private static String INSTANCE_UUID;

    /**
     * 一组键值对   用于描述服务器的信息
     */
    private static List<KeyStringValuePair> SERVICE_INSTANCE_PROPERTIES;

    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;
    private volatile RegisterGrpc.RegisterBlockingStub registerBlockingStub;
    private volatile ServiceInstancePingGrpc.ServiceInstancePingBlockingStub serviceInstancePingStub;
    private volatile ScheduledFuture<?> applicationRegisterFuture;
    private volatile long coolDownStartTime = -1;

    /**
     * 监听状态的变化
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

        // 填充属性
        for (String key : Config.Agent.INSTANCE_PROPERTIES.keySet()) {
            SERVICE_INSTANCE_PROPERTIES.add(KeyStringValuePair.newBuilder()
                                                              .setKey(key)
                                                              .setValue(Config.Agent.INSTANCE_PROPERTIES.get(key))
                                                              .build());
        }
    }

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

    @Override
    public void run() {
        logger.debug("ServiceAndEndpointRegisterClient running, status:{}.", status);

        // 应该是要等待多少时间后 才尝试连接服务器
        if (coolDownStartTime > 0) {
            final long coolDownDurationInMillis = TimeUnit.MINUTES.toMillis(Config.Agent.COOL_DOWN_THRESHOLD);
            if (System.currentTimeMillis() - coolDownStartTime < coolDownDurationInMillis) {
                logger.warn("The agent is cooling down, won't register itself");
                return;
            } else {
                logger.warn("The agent is re-registering itself to backend");
            }
        }
        coolDownStartTime = -1;

        boolean shouldTry = true;
        // 如果已经连接到服务器
        while (GRPCChannelStatus.CONNECTED.equals(status) && shouldTry) {
            shouldTry = false;
            try {
                if (RemoteDownstreamConfig.Agent.SERVICE_ID == DictionaryUtil.nullValue()) {
                    if (registerBlockingStub != null) {
                        // 将消费者注册到 注册中心  grpc 官方demo没有这一步啊
                        ServiceRegisterMapping serviceRegisterMapping = registerBlockingStub.withDeadlineAfter(
                            GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                        ).doServiceRegister(
                            Services.newBuilder()
                                    .addServices(
                                        Service
                                            .newBuilder()
                                            .setServiceName(Config.Agent.SERVICE_NAME)
                                            .setType(ServiceType.normal))
                                    .build()
                        );
                        if (serviceRegisterMapping != null) {
                            for (KeyIntValuePair registered : serviceRegisterMapping.getServicesList()) {
                                if (Config.Agent.SERVICE_NAME.equals(registered.getKey())) {
                                    RemoteDownstreamConfig.Agent.SERVICE_ID = registered.getValue();
                                    shouldTry = true;
                                }
                            }
                        }
                    }
                } else {
                    if (registerBlockingStub != null) {
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
                            for (KeyIntValuePair serviceInstance : instanceMapping.getServiceInstancesList()) {
                                if (INSTANCE_UUID.equals(serviceInstance.getKey())) {
                                    int serviceInstanceId = serviceInstance.getValue();
                                    if (serviceInstanceId != DictionaryUtil.nullValue()) {
                                        RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID = serviceInstanceId;
                                        RemoteDownstreamConfig.Agent.INSTANCE_REGISTERED_TIME = System.currentTimeMillis();
                                    }
                                }
                            }
                        } else {
                            final Commands commands = serviceInstancePingStub.withDeadlineAfter(
                                GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                            ).doPing(ServiceInstancePingPkg.newBuilder()
                                                           .setServiceInstanceId(
                                                               RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID)
                                                           .setTime(System.currentTimeMillis())
                                                           .setServiceInstanceUUID(INSTANCE_UUID)
                                                           .build());

                            NetworkAddressDictionary.INSTANCE.syncRemoteDictionary(
                                registerBlockingStub.withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS));
                            EndpointNameDictionary.INSTANCE.syncRemoteDictionary(
                                registerBlockingStub.withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS));
                            ServiceManager.INSTANCE.findService(CommandService.class).receiveCommand(commands);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error(t, "ServiceAndEndpointRegisterClient execute fail.");
                ServiceManager.INSTANCE.findService(GRPCChannelManager.class).reportError(t);
            }
        }
    }

    public void coolDown() {
        this.coolDownStartTime = System.currentTimeMillis();
    }
}
