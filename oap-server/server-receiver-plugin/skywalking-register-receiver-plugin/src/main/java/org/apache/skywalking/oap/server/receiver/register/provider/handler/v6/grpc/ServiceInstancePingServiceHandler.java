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

package org.apache.skywalking.oap.server.receiver.register.provider.handler.v6.grpc;

import io.grpc.stub.StreamObserver;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.network.common.Command;
import org.apache.skywalking.apm.network.common.Commands;
import org.apache.skywalking.apm.network.register.v2.ServiceInstancePingGrpc;
import org.apache.skywalking.apm.network.register.v2.ServiceInstancePingPkg;
import org.apache.skywalking.apm.network.trace.component.command.ServiceResetCommand;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.cache.ServiceInstanceInventoryCache;
import org.apache.skywalking.oap.server.core.command.CommandService;
import org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory;
import org.apache.skywalking.oap.server.core.register.service.IServiceInstanceInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.IServiceInventoryRegister;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.server.grpc.GRPCHandler;

/**
 * ServiceInstancePingServiceHandler responses the requests for instance ping. Trigger the heartbeat update and push the
 * commands to the downstream.
 * 该对象负责接收 oap 服务的心跳信息  那么应该会有某个地方定期让这些没有收到心跳的服务下线
 */
@Slf4j
public class ServiceInstancePingServiceHandler extends ServiceInstancePingGrpc.ServiceInstancePingImplBase implements GRPCHandler {
    private final ServiceInstanceInventoryCache serviceInstanceInventoryCache;
    private final IServiceInventoryRegister serviceInventoryRegister;
    private final IServiceInstanceInventoryRegister serviceInstanceInventoryRegister;
    /**
     * 该对象用于生成命令 影响client的行为
     */
    private final CommandService commandService;

    public ServiceInstancePingServiceHandler(ModuleManager moduleManager) {
        this.serviceInstanceInventoryCache = moduleManager.find(CoreModule.NAME)
                                                          .provider()
                                                          .getService(ServiceInstanceInventoryCache.class);
        this.serviceInventoryRegister = moduleManager.find(CoreModule.NAME)
                                                     .provider()
                                                     .getService(IServiceInventoryRegister.class);
        this.serviceInstanceInventoryRegister = moduleManager.find(CoreModule.NAME)
                                                             .provider()
                                                             .getService(IServiceInstanceInventoryRegister.class);
        this.commandService = moduleManager.find(CoreModule.NAME).provider().getService(CommandService.class);
    }

    /**
     * 代表接收到 client 心跳
     * @param request
     * @param responseObserver
     */
    @Override
    public void doPing(ServiceInstancePingPkg request, StreamObserver<Commands> responseObserver) {
        // 代表接收到哪个服务实例的信息
        int serviceInstanceId = request.getServiceInstanceId();
        long heartBeatTime = request.getTime();
        // 更新心跳时间并持久化
        serviceInstanceInventoryRegister.heartbeat(serviceInstanceId, heartBeatTime);

        ServiceInstanceInventory serviceInstanceInventory = serviceInstanceInventoryCache.get(serviceInstanceId);
        if (Objects.nonNull(serviceInstanceInventory)) {
            // 更新服务级别的心跳时间
            serviceInventoryRegister.heartbeat(serviceInstanceInventory.getServiceId(), heartBeatTime);
            responseObserver.onNext(Commands.getDefaultInstance());
        } else {
            log.warn(
                "Can't find service by service instance id from cache," + " service instance id is: {}, will send a reset command to agent side",
                serviceInstanceId
            );

            // 代表已经查询不到服务实例了  发起重置命令
            final ServiceResetCommand resetCommand = commandService.newResetCommand(
                request.getServiceInstanceId(), request
                    .getTime(), request.getServiceInstanceUUID());
            final Command command = resetCommand.serialize().build();
            final Commands nextCommands = Commands.newBuilder().addCommands(command).build();
            responseObserver.onNext(nextCommands);
        }

        responseObserver.onCompleted();
    }
}
