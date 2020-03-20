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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.Downsampling;
import org.apache.skywalking.oap.server.core.analysis.Stream;
import org.apache.skywalking.oap.server.core.analysis.StreamProcessor;
import org.apache.skywalking.oap.server.core.register.RegisterSource;
import org.apache.skywalking.oap.server.core.storage.IRegisterDAO;
import org.apache.skywalking.oap.server.core.storage.StorageDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.annotation.Storage;
import org.apache.skywalking.oap.server.core.storage.model.IModelSetter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.worker.IWorkerInstanceSetter;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;

/**
 * InventoryStreamProcessor represents the entrance and creator of the inventory register work flow.
 *
 * Method #in provides the major entrance for inventory streaming merge, eventually add or update the
 * inventory data in the storage.
 *
 * Method #create creates the workers and work flow for every inventory.
 * 库存处理器
 */
public class InventoryStreamProcessor implements StreamProcessor<RegisterSource> {
    /**
     * Singleton instance.
     * 单例模式
     */
    private static final InventoryStreamProcessor PROCESSOR = new InventoryStreamProcessor();

    /**
     * Worker table hosts all entrance workers.
     * 不同的数据源 对应不同的 worker
     */
    private Map<Class<? extends RegisterSource>, RegisterDistinctWorker> entryWorkers = new HashMap<>();

    public static InventoryStreamProcessor getInstance() {
        return PROCESSOR;
    }

    public void in(RegisterSource registerSource) {
        entryWorkers.get(registerSource.getClass()).in(registerSource);
    }

    /**
     * Create the workers and work flow for every inventory.
     *
     * @param moduleDefineHolder pointer of the module define.    ModuleManager
     * @param stream             definition of the inventory class.   携带的注解信息
     * @param inventoryClass     data type of the inventory.
     */
    @SuppressWarnings("unchecked")
    public void create(ModuleDefineHolder moduleDefineHolder, Stream stream,
                       Class<? extends RegisterSource> inventoryClass) {
        // 找到存储层组件
        StorageDAO storageDAO = moduleDefineHolder.find(StorageModule.NAME).provider().getService(StorageDAO.class);
        IRegisterDAO registerDAO;
        try {
            // registerSource 代表一类数据  这里通过builder().newInstance()  创建builder 实例 用于数据映射和转换
            // 这里返回的是存储层实例了
            registerDAO = storageDAO.newRegisterDao(stream.builder().newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnexpectedException("Create " + stream.builder().getSimpleName() + " register DAO failure.", e);
        }

        // 这里获取 StorageModels
        IModelSetter modelSetter = moduleDefineHolder.find(CoreModule.NAME).provider().getService(IModelSetter.class);
        // 保存映射关系
        Model model = modelSetter.putIfAbsent(
            inventoryClass, stream.scopeId(), new Storage(stream.name(), false, false, Downsampling.None), false);

        // 生成相关的worker 对象
        RegisterPersistentWorker persistentWorker = new RegisterPersistentWorker(
            moduleDefineHolder, model.getName(), registerDAO, stream
            .scopeId());

        String remoteReceiverWorkerName = stream.name() + "_rec";
        IWorkerInstanceSetter workerInstanceSetter = moduleDefineHolder.find(CoreModule.NAME)
                                                                       .provider()
                                                                       .getService(IWorkerInstanceSetter.class);
        // 这里添加 RegisterSource实例 与 worker的映射关系
        workerInstanceSetter.put(remoteReceiverWorkerName, persistentWorker, inventoryClass);

        // 应该是负责主从机同步的 RegisterRemoteWorker 内部包含一个 remoteSenderService 负责将数据带到远端
        RegisterRemoteWorker remoteWorker = new RegisterRemoteWorker(moduleDefineHolder, remoteReceiverWorkerName);

        RegisterDistinctWorker distinctWorker = new RegisterDistinctWorker(moduleDefineHolder, remoteWorker);

        entryWorkers.put(inventoryClass, distinctWorker);
    }

    /**
     * @return all register sourceScopeId class types
     */
    public List<Class> getAllRegisterSources() {
        List allSources = new ArrayList<>();
        entryWorkers.keySet().forEach(allSources::add);
        return allSources;
    }
}
