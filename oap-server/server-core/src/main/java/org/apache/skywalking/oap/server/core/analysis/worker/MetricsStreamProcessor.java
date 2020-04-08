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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.DisableRegister;
import org.apache.skywalking.oap.server.core.analysis.Downsampling;
import org.apache.skywalking.oap.server.core.analysis.Stream;
import org.apache.skywalking.oap.server.core.analysis.StreamProcessor;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.config.DownsamplingConfigService;
import org.apache.skywalking.oap.server.core.storage.IMetricsDAO;
import org.apache.skywalking.oap.server.core.storage.StorageDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.annotation.Storage;
import org.apache.skywalking.oap.server.core.storage.model.IModelSetter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.worker.IWorkerInstanceSetter;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;

/**
 * MetricsStreamProcessor represents the entrance and creator of the metrics streaming aggregation work flow.
 *
 * {@link #in(Metrics)} provides the major entrance for metrics streaming calculation.
 *
 * {@link #create(ModuleDefineHolder, Stream, Class)} creates the workers and work flow for every metrics.
 */
public class MetricsStreamProcessor implements StreamProcessor<Metrics> {
    /**
     * Singleton instance.
     */
    private final static MetricsStreamProcessor PROCESSOR = new MetricsStreamProcessor();

    /**
     * Worker table hosts all entrance workers.
     */
    private Map<Class<? extends Metrics>, MetricsAggregateWorker> entryWorkers = new HashMap<>();

    /**
     * Worker table hosts all persistent workers.
     */
    @Getter
    private List<MetricsPersistentWorker> persistentWorkers = new ArrayList<>();

    /**
     * Hold and forward CoreModuleConfig#enableDatabaseSession to the persistent worker.
     */
    @Setter
    @Getter
    private boolean enableDatabaseSession;

    public static MetricsStreamProcessor getInstance() {
        return PROCESSOR;
    }

    public void in(Metrics metrics) {
        MetricsAggregateWorker worker = entryWorkers.get(metrics.getClass());
        if (worker != null) {
            worker.in(metrics);
        }
    }

    /**
     * Create the workers and work flow for every metrics.
     *
     * @param moduleDefineHolder pointer of the module define.
     * @param stream             definition of the metrics class.
     * @param metricsClass       data type of the streaming calculation.
     */
    @SuppressWarnings("unchecked")
    public void create(ModuleDefineHolder moduleDefineHolder, Stream stream, Class<? extends Metrics> metricsClass) {
        if (DisableRegister.INSTANCE.include(stream.name())) {
            return;
        }

        StorageDAO storageDAO = moduleDefineHolder.find(StorageModule.NAME).provider().getService(StorageDAO.class);
        IMetricsDAO metricsDAO;
        try {
            metricsDAO = storageDAO.newMetricsDao(stream.builder().newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnexpectedException("Create " + stream.builder().getSimpleName() + " metrics DAO failure.", e);
        }

        IModelSetter modelSetter = moduleDefineHolder.find(CoreModule.NAME).provider().getService(IModelSetter.class);
        // 采样服务配置
        DownsamplingConfigService configService = moduleDefineHolder.find(CoreModule.NAME)
                                                                    .provider()
                                                                    .getService(DownsamplingConfigService.class);

        MetricsPersistentWorker hourPersistentWorker = null;
        MetricsPersistentWorker dayPersistentWorker = null;
        MetricsPersistentWorker monthPersistentWorker = null;

        if (configService.shouldToHour()) {
            Model model = modelSetter.putIfAbsent(
                metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Hour), false);
            // 根据 model 生成worker对象
            hourPersistentWorker = worker(moduleDefineHolder, metricsDAO, model);
        }
        if (configService.shouldToDay()) {
            Model model = modelSetter.putIfAbsent(
                metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Day), false);
            dayPersistentWorker = worker(moduleDefineHolder, metricsDAO, model);
        }
        if (configService.shouldToMonth()) {
            Model model = modelSetter.putIfAbsent(
                metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Month), false);
            monthPersistentWorker = worker(moduleDefineHolder, metricsDAO, model);
        }

        // 这里作为3个维度worker的入口
        MetricsTransWorker transWorker = new MetricsTransWorker(
            moduleDefineHolder, stream.name(), hourPersistentWorker, dayPersistentWorker, monthPersistentWorker);

        // 这里又创建一个 minute为维度的worker 对象
        Model model = modelSetter.putIfAbsent(
            metricsClass, stream.scopeId(), new Storage(stream.name(), true, true, Downsampling.Minute), false);
        MetricsPersistentWorker minutePersistentWorker = minutePersistentWorker(
            moduleDefineHolder, metricsDAO, model, transWorker);

        String remoteReceiverWorkerName = stream.name() + "_rec";
        IWorkerInstanceSetter workerInstanceSetter = moduleDefineHolder.find(CoreModule.NAME)
                                                                       .provider()
                                                                       .getService(IWorkerInstanceSetter.class);
        workerInstanceSetter.put(remoteReceiverWorkerName, minutePersistentWorker, metricsClass);

        // 内部包含一组client 会选择一个节点发送数据 包含本机   如果是本机的话会触发MetricsPersistentWorker 的逻辑
        MetricsRemoteWorker remoteWorker = new MetricsRemoteWorker(moduleDefineHolder, remoteReceiverWorkerName);
        // 该对象内部实现读写分离 和 批处理  之后将数据转发到 remoteWorker
        MetricsAggregateWorker aggregateWorker = new MetricsAggregateWorker(
            moduleDefineHolder, remoteWorker, stream.name());

        entryWorkers.put(metricsClass, aggregateWorker);
    }

    /**
     * 创建分钟为维度的worker
     * @param moduleDefineHolder
     * @param metricsDAO
     * @param model
     * @param transWorker
     * @return
     */
    private MetricsPersistentWorker minutePersistentWorker(ModuleDefineHolder moduleDefineHolder,
                                                           IMetricsDAO metricsDAO,
                                                           Model model,
                                                           MetricsTransWorker transWorker) {
        // 该对象用于接收数据并触发警报模块
        AlarmNotifyWorker alarmNotifyWorker = new AlarmNotifyWorker(moduleDefineHolder);
        // 该对象触发报告模块  也就是在配置文件中指定一个地址作为服务器 这里会将数据发送到那边
        ExportWorker exportWorker = new ExportWorker(moduleDefineHolder);

        MetricsPersistentWorker minutePersistentWorker = new MetricsPersistentWorker(
            moduleDefineHolder, model, metricsDAO, alarmNotifyWorker, exportWorker, transWorker, enableDatabaseSession);
        persistentWorkers.add(minutePersistentWorker);

        return minutePersistentWorker;
    }

    /**
     *
     * @param moduleDefineHolder  该对象的职责是便于获取需要的各个模块
     * @param metricsDAO  储存测量数据相关的持久层
     * @param model  具体存储的数据模型信息
     * @return
     */
    private MetricsPersistentWorker worker(ModuleDefineHolder moduleDefineHolder, IMetricsDAO metricsDAO, Model model) {
        MetricsPersistentWorker persistentWorker = new MetricsPersistentWorker(
            moduleDefineHolder, model, metricsDAO, null, null, null, enableDatabaseSession);
        persistentWorkers.add(persistentWorker);

        return persistentWorker;
    }
}
