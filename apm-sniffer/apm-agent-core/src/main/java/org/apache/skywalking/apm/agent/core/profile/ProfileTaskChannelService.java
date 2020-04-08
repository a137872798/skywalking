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

package org.apache.skywalking.apm.agent.core.profile;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelListener;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelManager;
import org.apache.skywalking.apm.agent.core.remote.GRPCChannelStatus;
import org.apache.skywalking.apm.agent.core.remote.GRPCStreamServiceStatus;
import org.apache.skywalking.apm.network.common.Commands;
import org.apache.skywalking.apm.network.language.profile.ProfileTaskCommandQuery;
import org.apache.skywalking.apm.network.language.profile.ProfileTaskFinishReport;
import org.apache.skywalking.apm.network.language.profile.ProfileTaskGrpc;
import org.apache.skywalking.apm.network.language.profile.ThreadSnapshot;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;

import static org.apache.skywalking.apm.agent.core.conf.Config.Collector.GRPC_UPSTREAM_TIMEOUT;

/**
 * Sniffer and backend, about the communication service of profile task protocol. 1. Sniffer will check has new profile
 * task list every {@link Config.Collector#GET_PROFILE_TASK_INTERVAL} second. 2. When there is a new profile task
 * snapshot, the data is transferred to the back end. use {@link LinkedBlockingQueue} 3. When profiling task finish, it
 * will send task finish status to backend
 * 该对象负责将 ProfileTask 数据发送到 oap 上
 */
@DefaultImplementor
public class ProfileTaskChannelService implements BootService, Runnable, GRPCChannelListener {
    private static final ILog logger = LogManager.getLogger(ProfileTaskChannelService.class);

    // channel status  代表channel的连接状态 默认是未连接
    private volatile GRPCChannelStatus status = GRPCChannelStatus.DISCONNECT;

    // gRPC stub  存根方法 类似于 dubbo 中消费者端开放的api
    private volatile ProfileTaskGrpc.ProfileTaskBlockingStub profileTaskBlockingStub;
    private volatile ProfileTaskGrpc.ProfileTaskStub profileTaskStub;

    // segment snapshot sender  存放线程堆栈信息的快照对象
    private final BlockingQueue<TracingThreadSnapshot> snapshotQueue = new LinkedBlockingQueue<>(
        Config.Profile.SNAPSHOT_TRANSPORT_BUFFER_SIZE);
    private volatile ScheduledFuture<?> sendSnapshotFuture;

    // query task list schedule
    private volatile ScheduledFuture<?> getTaskListFuture;

    /**
     * 当确认开启了 profile服务时  该方法由定时器执行
     */
    @Override
    public void run() {
        // 确保此时服务信息已经注册到了oap上
        if (RemoteDownstreamConfig.Agent.SERVICE_ID != DictionaryUtil.nullValue()
            && RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID != DictionaryUtil.nullValue()) {
            // 此时通道是可用的
            if (status == GRPCChannelStatus.CONNECTED) {
                try {
                    ProfileTaskCommandQuery.Builder builder = ProfileTaskCommandQuery.newBuilder();

                    // sniffer info  标注本次描述信息从哪个服务实例传来
                    builder.setServiceId(RemoteDownstreamConfig.Agent.SERVICE_ID)
                           .setInstanceId(RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID);

                    // last command create time
                    builder.setLastCommandTime(ServiceManager.INSTANCE.findService(ProfileTaskExecutionService.class)
                                                                      .getLastCommandCreateTime());

                    // 调用 getProfileTaskCommands 实际会调用到 oap 下某个GRPCHandler 对应的方法
                    Commands commands = profileTaskBlockingStub.withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS)
                                                               .getProfileTaskCommands(builder.build());

                    // 转发给对应 command 处理器
                    ServiceManager.INSTANCE.findService(CommandService.class).receiveCommand(commands);
                } catch (Throwable t) {
                    if (!(t instanceof StatusRuntimeException)) {
                        logger.error(t, "Query profile task from backend fail.");
                        return;
                    }
                    final StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
                    if (statusRuntimeException.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
                        logger.warn("Backend doesn't support profiling, profiling will be disabled");
                        if (getTaskListFuture != null) {
                            getTaskListFuture.cancel(true);
                        }

                        // stop snapshot sender
                        if (sendSnapshotFuture != null) {
                            sendSnapshotFuture.cancel(true);
                        }
                    }
                }
            }
        }

    }

    @Override
    public void prepare() {
        ServiceManager.INSTANCE.findService(GRPCChannelManager.class).addChannelListener(this);
    }

    /**
     * 通过 GRPC 将信息发布到 oap 上
     */
    @Override
    public void boot() {
        // 描述信息是否被激活
        if (Config.Profile.ACTIVE) {
            // query task list
            getTaskListFuture = Executors.newSingleThreadScheduledExecutor(
                new DefaultNamedThreadFactory("ProfileGetTaskService")
            ).scheduleWithFixedDelay(
                new RunnableWithExceptionProtection(
                    this,
                    t -> logger.error("Query profile task list failure.", t)
                ), 0, Config.Collector.GET_PROFILE_TASK_INTERVAL, TimeUnit.SECONDS
            );

            // profile 信息收集起来后会存放在该对象 并通过 GRPC 发送到 oap上
            sendSnapshotFuture = Executors.newSingleThreadScheduledExecutor(
                new DefaultNamedThreadFactory("ProfileSendSnapshotService")
            ).scheduleWithFixedDelay(
                new RunnableWithExceptionProtection(
                    new SnapshotSender(),
                    t -> logger.error("Profile segment snapshot upload failure.", t)
                ), 0, 500, TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    public void onComplete() {
    }

    @Override
    public void shutdown() {
        if (getTaskListFuture != null) {
            getTaskListFuture.cancel(true);
        }

        if (sendSnapshotFuture != null) {
            sendSnapshotFuture.cancel(true);
        }
    }

    /**
     * 当监听到连接完成时 创建存根对象用于与 oap 通信
     * @param status
     */
    @Override
    public void statusChanged(GRPCChannelStatus status) {
        if (GRPCChannelStatus.CONNECTED.equals(status)) {
            Channel channel = ServiceManager.INSTANCE.findService(GRPCChannelManager.class).getChannel();
            profileTaskBlockingStub = ProfileTaskGrpc.newBlockingStub(channel);
            profileTaskStub = ProfileTaskGrpc.newStub(channel);
        } else {
            profileTaskBlockingStub = null;
            profileTaskStub = null;
        }
        this.status = status;
    }

    /**
     * add a new profiling snapshot, send to {@link #snapshotQueue}
     * 每次 profiler 完成任务时 就会将快照信息保存到该对象中
     */
    public void addProfilingSnapshot(TracingThreadSnapshot snapshot) {
        snapshotQueue.add(snapshot);
    }

    /**
     * notify backend profile task has finish
     * 当某个任务完成时触发  需要通知到 oap
     */
    public void notifyProfileTaskFinish(ProfileTask task) {
        try {
            final ProfileTaskFinishReport.Builder reportBuilder = ProfileTaskFinishReport.newBuilder();
            // sniffer info
            reportBuilder.setServiceId(RemoteDownstreamConfig.Agent.SERVICE_ID)
                         .setInstanceId(RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID);
            // task info
            reportBuilder.setTaskId(task.getTaskId());

            // send data
            profileTaskBlockingStub.withDeadlineAfter(GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS)
                                   .reportTaskFinish(reportBuilder.build());
        } catch (Throwable e) {
            logger.error(e, "Notify profile task finish to backend fail.");
        }
    }

    /**
     * send segment snapshot
     * 每隔一定时间 将阻塞队列内的数据发送到 oap
     */
    private class SnapshotSender implements Runnable {

        @Override
        public void run() {
            if (status == GRPCChannelStatus.CONNECTED) {
                try {
                    ArrayList<TracingThreadSnapshot> buffer = new ArrayList<>(Config.Profile.SNAPSHOT_TRANSPORT_BUFFER_SIZE);
                    snapshotQueue.drainTo(buffer);
                    if (buffer.size() > 0) {
                        final GRPCStreamServiceStatus status = new GRPCStreamServiceStatus(false);
                        StreamObserver<ThreadSnapshot> snapshotStreamObserver = profileTaskStub.withDeadlineAfter(
                            GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                        ).collectSnapshot(
                            new StreamObserver<Commands>() {
                                @Override
                                public void onNext(
                                    Commands commands) {
                                }

                                @Override
                                public void onError(
                                    Throwable throwable) {
                                    status.finished();
                                    if (logger.isErrorEnable()) {
                                        logger.error(
                                            throwable,
                                            "Send profile segment snapshot to collector fail with a grpc internal exception."
                                        );
                                    }
                                    ServiceManager.INSTANCE.findService(GRPCChannelManager.class)
                                                           .reportError(throwable);
                                }

                                @Override
                                public void onCompleted() {
                                    status.finished();
                                }
                            }
                        );
                        // 将元素 通过 stream 形式传播到下游
                        for (TracingThreadSnapshot snapshot : buffer) {
                            final ThreadSnapshot transformSnapshot = snapshot.transform();
                            snapshotStreamObserver.onNext(transformSnapshot);
                        }

                        snapshotStreamObserver.onCompleted();
                        status.wait4Finish();
                    }
                } catch (Throwable t) {
                    logger.error(t, "Send profile segment snapshot to backend fail.");
                }
            }
        }

    }
}
