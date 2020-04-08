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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;

@DefaultImplementor
public class GRPCChannelManager implements BootService, Runnable {
    private static final ILog logger = LogManager.getLogger(GRPCChannelManager.class);

    /**
     * 一个manager 对象 只对应一个channel
     */
    private volatile GRPCChannel managedChannel = null;
    private volatile ScheduledFuture<?> connectCheckFuture;
    /**
     * 是否开启重连功能  首次启动就是通过该标识
     */
    private volatile boolean reconnect = true;
    private final Random random = new Random();
    /**
     * 状态变化的监听器
     */
    private final List<GRPCChannelListener> listeners = Collections.synchronizedList(new LinkedList<>());
    /**
     * 需要连接的服务器地址  每次只连接一个 当出现异常时 才选择更换
     */
    private volatile List<String> grpcServers;
    private volatile int selectedIdx = -1;
    /**
     * 代表重试次数
     */
    private volatile int reconnectCount = 0;

    @Override
    public void prepare() {

    }

    /**
     * 当插入探针的应用启动时 就会开启client 并将实时监控到的信息发送到远端服务器
     */
    @Override
    public void boot() {
        // 如果没有设置 后端服务器 那么就不需要发送数据了
        if (Config.Collector.BACKEND_SERVICE.trim().length() == 0) {
            logger.error("Collector server addresses are not set.");
            // 代表代理对象不会将任何数据上传到oap服务器
            logger.error("Agent will not uplink any data.");
            return;
        }
        grpcServers = Arrays.asList(Config.Collector.BACKEND_SERVICE.split(","));
        connectCheckFuture = Executors.newSingleThreadScheduledExecutor(
                new DefaultNamedThreadFactory("GRPCChannelManager")
        ).scheduleAtFixedRate(
                // 该对象会捕获出现的异常 比使用降级策略来处理 对应到这里就是 logger.error
                new RunnableWithExceptionProtection(
                        this,
                        t -> logger.error("unexpected exception.", t)
                ), 0, Config.Collector.GRPC_CHANNEL_CHECK_INTERVAL, TimeUnit.SECONDS
        );
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {
        if (connectCheckFuture != null) {
            connectCheckFuture.cancel(true);
        }
        // 应该是对应nettyChannel
        if (managedChannel != null) {
            managedChannel.shutdownNow();
        }
        logger.debug("Selected collector grpc service shutdown.");
    }

    /**
     * 在定时器中 执行该任务 用于确保连接
     */
    @Override
    public void run() {
        logger.debug("Selected collector grpc service running, reconnect:{}.", reconnect);
        // 当出现了某些异常时  该标识会设置成true
        if (reconnect) {
            // 并不是连接到所有server 而是会通过轮询策略选择其中一个地址   假设上面的一组地址 属于同一个 skywalking集群
            // 那么集群内所有接收到的数据本身就可以通过 ForeverFirst 集中到某一个节点 所以这里不需要维护所有远端服务器的连接
            if (grpcServers.size() > 0) {
                String server = "";
                try {
                    int index = Math.abs(random.nextInt()) % grpcServers.size();
                    if (index != selectedIdx) {
                        selectedIdx = index;

                        server = grpcServers.get(index);
                        String[] ipAndPort = server.split(":");

                        // 当出现异常时 先关闭之前的连接
                        if (managedChannel != null) {
                            managedChannel.shutdownNow();
                        }

                        // 这里应该没有真正完成连接吧
                        managedChannel = GRPCChannel.newBuilder(ipAndPort[0], Integer.parseInt(ipAndPort[1]))
                                // 这2个对象用于配置channel
                                .addManagedChannelBuilder(new StandardChannelBuilder())
                                .addManagedChannelBuilder(new TLSChannelBuilder())
                                // 这2个对象用于装饰channel
                                .addChannelDecorator(new AgentIDDecorator())
                                .addChannelDecorator(new AuthenticationDecorator())
                                .build();
                        // 某些 service 会设置监听器到该对象上 根据channel 对象 创建存根对象
                        notify(GRPCChannelStatus.CONNECTED);
                        // 当连接到某个服务后 重置相关标识
                        reconnectCount = 0;
                        reconnect = false;
                        // 这里是触发了 底层 GRPC 的重连机制   当传入标识为false 时 代表不尝试重连 那么返回无连接状态时 等待触发下次任务
                    } else if (managedChannel.isConnected(++reconnectCount > Config.Agent.FORCE_RECONNECTION_PERIOD)) {
                        // Reconnect to the same server is automatically done by GRPC,
                        // therefore we are responsible to check the connectivity and
                        // set the state and notify listeners
                        reconnectCount = 0;
                        notify(GRPCChannelStatus.CONNECTED);
                        reconnect = false;
                    }

                    return;
                } catch (Throwable t) {
                    logger.error(t, "Create channel to {} fail.", server);
                }
            }

            logger.debug(
                    "Selected collector grpc service is not available. Wait {} seconds to retry",
                    Config.Collector.GRPC_CHANNEL_CHECK_INTERVAL
            );
        }
    }

    public void addChannelListener(GRPCChannelListener listener) {
        listeners.add(listener);
    }

    /**
     * 获取装饰完的channel 对象
     *
     * @return
     */
    public Channel getChannel() {
        return managedChannel.getChannel();
    }

    /**
     * If the given expcetion is triggered by network problem, connect in background.
     * 当检测到网络异常时 才标记需要重连 然后在定时任务中会重新创建连接
     */
    public void reportError(Throwable throwable) {
        if (isNetworkError(throwable)) {
            reconnect = true;
            notify(GRPCChannelStatus.DISCONNECT);
        }
    }

    private void notify(GRPCChannelStatus status) {
        for (GRPCChannelListener listener : listeners) {
            try {
                listener.statusChanged(status);
            } catch (Throwable t) {
                logger.error(t, "Fail to notify {} about channel connected.", listener.getClass().getName());
            }
        }
    }

    /**
     * 判断产生的异常是否是网络异常
     *
     * @param throwable
     * @return
     */
    private boolean isNetworkError(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) throwable;
            return statusEquals(
                    statusRuntimeException.getStatus(), Status.UNAVAILABLE, Status.PERMISSION_DENIED,
                    Status.UNAUTHENTICATED, Status.RESOURCE_EXHAUSTED, Status.UNKNOWN
            );
        }
        return false;
    }

    private boolean statusEquals(Status sourceStatus, Status... potentialStatus) {
        for (Status status : potentialStatus) {
            if (sourceStatus.getCode() == status.getCode()) {
                return true;
            }
        }
        return false;
    }
}
