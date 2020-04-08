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

package org.apache.skywalking.oap.server.core.analysis.worker;

import java.util.HashMap;
import java.util.Map;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.DisableRegister;
import org.apache.skywalking.oap.server.core.analysis.Downsampling;
import org.apache.skywalking.oap.server.core.analysis.Stream;
import org.apache.skywalking.oap.server.core.analysis.StreamProcessor;
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.storage.IRecordDAO;
import org.apache.skywalking.oap.server.core.storage.StorageDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.annotation.Storage;
import org.apache.skywalking.oap.server.core.storage.model.IModelSetter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;

/**
 * 处理record 类型的数据
 */
public class RecordStreamProcessor implements StreamProcessor<Record> {

    private final static RecordStreamProcessor PROCESSOR = new RecordStreamProcessor();

    /**
     * 不同的记录类型对应不同的 worker
     */
    private Map<Class<? extends Record>, RecordPersistentWorker> workers = new HashMap<>();

    public static RecordStreamProcessor getInstance() {
        return PROCESSOR;
    }

    public void in(Record record) {
        // 首先尝试从 映射中根据 record类型找到处理器 找到的时候将数据传入
        RecordPersistentWorker worker = workers.get(record.getClass());
        if (worker != null) {
            worker.in(record);
        }
    }

    /**
     * 按照某种记录类型生成对应的处理器
     * @param moduleDefineHolder
     * @param stream
     * @param recordClass
     */
    @SuppressWarnings("unchecked")
    public void create(ModuleDefineHolder moduleDefineHolder, Stream stream, Class<? extends Record> recordClass) {
        if (DisableRegister.INSTANCE.include(stream.name())) {
            return;
        }

        // 找到存储模块的 存储服务  (dao代表与持久化工具交互的那层)
        StorageDAO storageDAO = moduleDefineHolder.find(StorageModule.NAME).provider().getService(StorageDAO.class);
        IRecordDAO recordDAO;
        try {
            // 生成持久 record 的对象
            recordDAO = storageDAO.newRecordDao(stream.builder().newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnexpectedException("Create " + stream.builder().getSimpleName() + " record DAO failure.", e);
        }

        // 找到 modelSet 模块   它相当于 管理整个统计数据相关的模型
        IModelSetter modelSetter = moduleDefineHolder.find(CoreModule.NAME).provider().getService(IModelSetter.class);
        Model model = modelSetter.putIfAbsent(recordClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Second), true);
        RecordPersistentWorker persistentWorker = new RecordPersistentWorker(moduleDefineHolder, model, recordDAO);

        workers.put(recordClass, persistentWorker);
    }
}
