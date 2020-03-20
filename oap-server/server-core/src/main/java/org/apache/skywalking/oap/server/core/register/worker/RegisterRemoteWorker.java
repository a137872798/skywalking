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

package org.apache.skywalking.oap.server.core.register.worker;

import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.register.RegisterSource;
import org.apache.skywalking.oap.server.core.remote.RemoteSenderService;
import org.apache.skywalking.oap.server.core.remote.selector.Selector;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 远端工作者
 */
public class RegisterRemoteWorker extends AbstractWorker<RegisterSource> {

    private static final Logger logger = LoggerFactory.getLogger(RegisterRemoteWorker.class);

    /**
     * 远端工作者的名字
     */
    private final String remoteReceiverWorkerName;

    /**
     * 多个 registerRemoteWorker 指向的是同一个 remoteSenderService
     */
    private final RemoteSenderService remoteSender;

    RegisterRemoteWorker(ModuleDefineHolder moduleDefineHolder, String remoteReceiverWorkerName) {
        super(moduleDefineHolder);
        // 获取发送数据的服务  它代表本节点是 client 节点
        this.remoteSender = moduleDefineHolder.find(CoreModule.NAME).provider().getService(RemoteSenderService.class);
        this.remoteReceiverWorkerName = remoteReceiverWorkerName;
    }

    /**
     * 当接收到数据时  同时发送到远端
     * @param registerSource
     */
    @Override
    public final void in(RegisterSource registerSource) {
        try {
            remoteSender.send(remoteReceiverWorkerName, registerSource, Selector.ForeverFirst);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }
}
