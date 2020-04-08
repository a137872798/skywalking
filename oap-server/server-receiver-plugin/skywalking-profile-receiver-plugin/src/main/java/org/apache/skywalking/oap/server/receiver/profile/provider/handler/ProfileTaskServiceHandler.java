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

package org.apache.skywalking.oap.server.receiver.profile.provider.handler;

import io.grpc.stub.StreamObserver;
import org.apache.skywalking.apm.network.common.Commands;
import org.apache.skywalking.apm.network.language.agent.UniqueId;
import org.apache.skywalking.apm.network.language.profile.ProfileTaskCommandQuery;
import org.apache.skywalking.apm.network.language.profile.ProfileTaskFinishReport;
import org.apache.skywalking.apm.network.language.profile.ProfileTaskGrpc;
import org.apache.skywalking.apm.network.language.profile.ThreadSnapshot;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.analysis.TimeBucket;
import org.apache.skywalking.oap.server.core.analysis.worker.RecordStreamProcessor;
import org.apache.skywalking.oap.server.core.cache.ProfileTaskCache;
import org.apache.skywalking.oap.server.core.command.CommandService;
import org.apache.skywalking.oap.server.core.profile.ProfileTaskLogRecord;
import org.apache.skywalking.oap.server.core.profile.ProfileThreadSnapshotRecord;
import org.apache.skywalking.oap.server.core.query.entity.ProfileTask;
import org.apache.skywalking.oap.server.core.query.entity.ProfileTaskLogOperationType;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.server.grpc.GRPCHandler;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ProfileTaskServiceHandler extends ProfileTaskGrpc.ProfileTaskImplBase implements GRPCHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProfileTaskServiceHandler.class);

    private ProfileTaskCache profileTaskCache;
    private final CommandService commandService;

    public ProfileTaskServiceHandler(ModuleManager moduleManager) {
        this.profileTaskCache = moduleManager.find(CoreModule.NAME).provider().getService(ProfileTaskCache.class);
        this.commandService = moduleManager.find(CoreModule.NAME).provider().getService(CommandService.class);
    }

    /**
     * 植入探针的程序 会定期判断是否生成了最新的 profile 任务
     * @param request
     * @param responseObserver
     */
    @Override
    public void getProfileTaskCommands(ProfileTaskCommandQuery request, StreamObserver<Commands> responseObserver) {
        // query profile task list by service id
        // 获取当前待执行的 profileTask   TODO 数据是什么时候添加到 dao 的
        final List<ProfileTask> profileTaskList = profileTaskCache.getProfileTaskList(request.getServiceId());
        // 没有任务 返回一个空的command
        if (CollectionUtils.isEmpty(profileTaskList)) {
            responseObserver.onNext(Commands.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        // build command list
        final Commands.Builder commandsBuilder = Commands.newBuilder();
        final long lastCommandTime = request.getLastCommandTime();

        // 这里过滤掉 过早的任务
        for (ProfileTask profileTask : profileTaskList) {
            // if command create time less than last command time, means sniffer already have task
            if (profileTask.getCreateTime() <= lastCommandTime) {
                continue;
            }

            // record profile task log   存储 logRecord
            recordProfileTaskLog(profileTask, request.getInstanceId(), ProfileTaskLogOperationType.NOTIFIED);

            // add command   添加一个待执行任务
            commandsBuilder.addCommands(commandService.newProfileTaskCommand(profileTask).serialize().build());
        }

        responseObserver.onNext(commandsBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * 当 植入了 探针的程序接收到 oap下发的profile任务时  定期收集当前线程栈信息 并上发到 oap
     * @param responseObserver
     * @return
     */
    @Override
    public StreamObserver<ThreadSnapshot> collectSnapshot(StreamObserver<Commands> responseObserver) {
        return new StreamObserver<ThreadSnapshot>() {
            @Override
            public void onNext(ThreadSnapshot snapshot) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("receive profile segment snapshot");
                }

                // parse segment id   当前快照对应链路id
                UniqueId uniqueId = snapshot.getTraceSegmentId();
                StringBuilder segmentIdBuilder = new StringBuilder();
                for (int i = 0; i < uniqueId.getIdPartsList().size(); i++) {
                    if (i == 0) {
                        segmentIdBuilder.append(uniqueId.getIdPartsList().get(i));
                    } else {
                        segmentIdBuilder.append(".").append(uniqueId.getIdPartsList().get(i));
                    }
                }

                // build database data
                final ProfileThreadSnapshotRecord record = new ProfileThreadSnapshotRecord();
                record.setTaskId(snapshot.getTaskId());
                record.setSegmentId(segmentIdBuilder.toString());
                record.setDumpTime(snapshot.getTime());
                record.setSequence(snapshot.getSequence());
                record.setStackBinary(snapshot.getStack().toByteArray());
                record.setTimeBucket(TimeBucket.getRecordTimeBucket(snapshot.getTime()));

                // async storage
                RecordStreamProcessor.getInstance().in(record);
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.error(throwable.getMessage(), throwable);
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(Commands.newBuilder().build());
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * oap 定期下发 profileTask 到植入探针的程序 之后程序会追踪当前数据 并在执行完成时 将信息重新返回给 oap
     * @param request
     * @param responseObserver
     */
    @Override
    public void reportTaskFinish(ProfileTaskFinishReport request, StreamObserver<Commands> responseObserver) {
        // query task from cache, set log time bucket need it
        final ProfileTask profileTask = profileTaskCache.getProfileTaskById(request.getTaskId());

        // record finish log  追加一条操作完成的记录
        if (profileTask != null) {
            recordProfileTaskLog(profileTask, request.getInstanceId(), ProfileTaskLogOperationType.EXECUTION_FINISHED);
        }

        responseObserver.onNext(Commands.newBuilder().build());
        responseObserver.onCompleted();
    }

    /**
     * 生成 log 对象并保存
     * @param task
     * @param instanceId
     * @param operationType
     */
    private void recordProfileTaskLog(ProfileTask task, int instanceId, ProfileTaskLogOperationType operationType) {
        final ProfileTaskLogRecord logRecord = new ProfileTaskLogRecord();
        logRecord.setTaskId(task.getId());
        logRecord.setInstanceId(instanceId);
        logRecord.setOperationType(operationType.getCode());
        logRecord.setOperationTime(System.currentTimeMillis());
        // same with task time bucket, ensure record will ttl same with profile task
        logRecord.setTimeBucket(TimeBucket.getRecordTimeBucket(task.getStartTime() + TimeUnit.MINUTES.toMillis(task.getDuration())));

        RecordStreamProcessor.getInstance().in(logRecord);
    }

}
