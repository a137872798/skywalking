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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.consumer.BulkConsumePool;
import org.apache.skywalking.apm.commons.datacarrier.consumer.ConsumerPoolFactory;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.register.RegisterSource;
import org.apache.skywalking.oap.server.core.source.DefaultScopeDefine;
import org.apache.skywalking.oap.server.core.storage.IRegisterDAO;
import org.apache.skywalking.oap.server.core.storage.IRegisterLockDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 该对象专门用于统计 RegisterSource 的数据
 * 每一种具体的数据 都对应一个worker
 */
public class RegisterPersistentWorker extends AbstractWorker<RegisterSource> {

    private static final Logger logger = LoggerFactory.getLogger(RegisterPersistentWorker.class);

    /**
     * 某种具体数据的scopeId
     */
    private final int scopeId;
    private final String modelName;
    private final Map<RegisterSource, RegisterSource> sources;
    private final IRegisterLockDAO registerLockDAO;
    private final IRegisterDAO registerDAO;
    private final DataCarrier<RegisterSource> dataCarrier;
    private final HistogramMetrics workerLatencyHistogram;

    RegisterPersistentWorker(ModuleDefineHolder moduleDefineHolder, String modelName, IRegisterDAO registerDAO,
        int scopeId) {
        super(moduleDefineHolder);
        this.modelName = modelName;
        this.sources = new HashMap<>();
        this.registerDAO = registerDAO;
        // 获取锁服务   对应数据库的情况就是 for update
        // 实际上存储数据的 和 获取锁的是2个表  一个数据表 和一个锁表 通过锁表的行锁能起到对整张数据表的锁定  锁表通过scopeId 确定目标行
        this.registerLockDAO = moduleDefineHolder.find(StorageModule.NAME)
                                                 .provider()
                                                 .getService(IRegisterLockDAO.class);
        this.scopeId = scopeId;
        // 该对象是一个消费者/生产者组件 可以往内部添加数据 以及设置消费者来消费  每种具体的数据有对应的 dataCarrier 以及 worker
        this.dataCarrier = new DataCarrier<>("MetricsPersistentWorker." + modelName, 1, 1000);
        // 获取遥感服务 这里依赖第三方组件就不细看了
        MetricsCreator metricsCreator = moduleDefineHolder.find(TelemetryModule.NAME)
                                                          .provider()
                                                          .getService(MetricsCreator.class);

        // 这里只是创建了  柱形图对象 数据的统计在什么时候
        workerLatencyHistogram = metricsCreator.createHistogramMetric("register_persistent_worker_latency", "The process latency of register persistent worker", new MetricsTag.Keys("module"), new MetricsTag.Values(modelName));

        String name = "REGISTER_L2";
        int size = BulkConsumePool.Creator.recommendMaxSize() / 8;
        if (size == 0) {
            size = 1;
        }
        BulkConsumePool.Creator creator = new BulkConsumePool.Creator(name, size, 200);
        try {
            ConsumerPoolFactory.INSTANCE.createIfAbsent(name, creator);
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage(), e);
        }

        // 指定消费者 用于消费数据
        this.dataCarrier.consume(ConsumerPoolFactory.INSTANCE.get(name), new RegisterPersistentWorker.PersistentConsumer(this));
    }

    @Override
    public final void in(RegisterSource registerSource) {
        registerSource.resetEndOfBatch();
        dataCarrier.produce(registerSource);
    }

    /**
     * 消费逻辑转发到这里   核心思路就是 update registerSource 或者 insert registerSource
     * @param registerSource
     */
    private void onWork(RegisterSource registerSource) {
        // 想批量消费吧 那么也代表着数据的产生非常频繁
        if (!sources.containsKey(registerSource)) {
            sources.put(registerSource, registerSource);
        } else {
            sources.get(registerSource).combine(registerSource);
        }

        // 创建统计对象
        try (HistogramMetrics.Timer timer = workerLatencyHistogram.createTimer()) {
            // 此时存储的数据比较多了 或者 本次拉取到的数据是最后一个   每当生成一批数据待消费时 在消费过程中 又会有一批新的数据生成
            if (sources.size() > 1000 || registerSource.isEndOfBatch()) {
                sources.values().forEach(source -> {
                    try {
                        // 看来这里能够确保产生数据的时候 一定已经插入到表中了
                        // DataCarrier 是如何处理 生产者消费者速率不一致的问题的   首先消费者过快 使用阻塞的思路
                        // 而当生产者过快 通过赋予QueueBuffer 阻塞的能力 同样通过阻塞避免OOM   (当然还有其他拒绝策略)
                        // 背压的话 是由消费者主动申请 onNext 使得上游发射数据
                        // source.id 代表某个具体的数据
                        // TODO 这里要确认 每次生成的 source id 都是不同的 还是有可能会出现一致的情况  应该这样想 registerSource 代表一个注册源 那么那个注册源  肯定是具备更新的能力的
                        RegisterSource dbSource = registerDAO.get(modelName, source.id());
                        if (Objects.nonNull(dbSource)) {
                            if (dbSource.combine(source)) {
                                registerDAO.forceUpdate(modelName, dbSource);
                            }
                        } else {
                            // 代表新插入一个数据  插入的时候申请表锁 那么这里有性能瓶颈
                            int sequence;
                            if ((sequence = registerLockDAO.getId(scopeId, source)) != Const.NONE) {
                                try {
                                    dbSource = registerDAO.get(modelName, source.id());
                                    if (Objects.nonNull(dbSource)) {
                                        if (dbSource.combine(source)) {
                                            registerDAO.forceUpdate(modelName, dbSource);
                                        }
                                    } else {
                                        source.setSequence(sequence);
                                        registerDAO.forceInsert(modelName, source);
                                    }
                                } catch (Throwable t) {
                                    logger.error(t.getMessage(), t);
                                }
                            } else {
                                logger.info("{} inventory register try lock and increment sequence failure.", DefaultScopeDefine
                                    .nameOf(scopeId));
                            }
                        }
                    } catch (Throwable t) {
                        logger.error(t.getMessage(), t);
                    }
                });
                sources.clear();
            }
        }
    }

    /**
     * 该对象负责消费 遥感组件监控到的数据
     */
    private class PersistentConsumer implements IConsumer<RegisterSource> {

        private final RegisterPersistentWorker persistent;

        private PersistentConsumer(RegisterPersistentWorker persistent) {
            this.persistent = persistent;
        }

        @Override
        public void init() {

        }

        @Override
        public void consume(List<RegisterSource> data) {
            Iterator<RegisterSource> sourceIterator = data.iterator();

            int i = 0;
            while (sourceIterator.hasNext()) {
                RegisterSource registerSource = sourceIterator.next();
                i++;
                if (i == data.size()) {
                    registerSource.asEndOfBatch();
                }
                persistent.onWork(registerSource);
            }
        }

        @Override
        public void onError(List<RegisterSource> data, Throwable t) {
            logger.error(t.getMessage(), t);
        }

        @Override
        public void onExit() {
        }
    }
}
