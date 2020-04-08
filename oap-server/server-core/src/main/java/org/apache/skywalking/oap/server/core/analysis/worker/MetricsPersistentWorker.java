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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.consumer.BulkConsumePool;
import org.apache.skywalking.apm.commons.datacarrier.consumer.ConsumerPoolFactory;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.data.MergeDataCache;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.exporter.ExportEvent;
import org.apache.skywalking.oap.server.core.storage.IMetricsDAO;
import org.apache.skywalking.oap.server.core.storage.annotation.IDColumn;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.client.request.PrepareRequest;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;

/**
 * MetricsPersistentWorker is an extension of {@link PersistenceWorker} and focuses on the Metrics data persistent.
 * 负责将 测量数据进行持久化
 * 该对象的收集数据和 持久化是异步的
 */
@Slf4j
public class MetricsPersistentWorker extends PersistenceWorker<Metrics, MergeDataCache<Metrics>> {
    private final Model model;
    private final Map<Metrics, Metrics> databaseSession;
    private final MergeDataCache<Metrics> mergeDataCache;
    /**
     * 负责存储数据的持久层
     */
    private final IMetricsDAO metricsDAO;
    // 下面2个对象只有在 minute级别的测量数据时 才会设置
    /**
     * 对应警报模块
     */
    private final AbstractWorker<Metrics> nextAlarmWorker;
    /**
     * 对应报告模块
     */
    private final AbstractWorker<ExportEvent> nextExportWorker;
    private final DataCarrier<Metrics> dataCarrier;
    /**
     * 该对象作为 按 月 日 小时 为级别存储统计数据
     */
    private final MetricsTransWorker transWorker;
    private final boolean enableDatabaseSession;

    /**
     *
     * @param moduleDefineHolder
     * @param model
     * @param metricsDAO
     * @param nextAlarmWorker
     * @param nextExportWorker
     * @param transWorker
     * @param enableDatabaseSession  这啥意思???
     */
    MetricsPersistentWorker(ModuleDefineHolder moduleDefineHolder, Model model, IMetricsDAO metricsDAO,
                            AbstractWorker<Metrics> nextAlarmWorker, AbstractWorker<ExportEvent> nextExportWorker,
                            MetricsTransWorker transWorker, boolean enableDatabaseSession) {
        super(moduleDefineHolder);
        this.model = model;
        this.databaseSession = new HashMap<>(100);
        this.enableDatabaseSession = enableDatabaseSession;
        // 该容器内部有 window 的概念
        this.mergeDataCache = new MergeDataCache<>();
        this.metricsDAO = metricsDAO;
        this.nextAlarmWorker = nextAlarmWorker;
        this.nextExportWorker = nextExportWorker;
        this.transWorker = transWorker;

        String name = "METRICS_L2_AGGREGATION";
        int size = BulkConsumePool.Creator.recommendMaxSize() / 8;
        if (size == 0) {
            size = 1;
        }
        BulkConsumePool.Creator creator = new BulkConsumePool.Creator(name, size, 20);
        try {
            ConsumerPoolFactory.INSTANCE.createIfAbsent(name, creator);
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage(), e);
        }

        this.dataCarrier = new DataCarrier<>("MetricsPersistentWorker." + model.getName(), name, 1, 2000);
        this.dataCarrier.consume(ConsumerPoolFactory.INSTANCE.get(name), new PersistentConsumer(this));
    }

    /**
     * 当感知到测量数据时触发的函数
     * @param metrics
     */
    @Override
    void onWork(Metrics metrics) {
        cacheData(metrics);
    }

    /**
     * Accept all metrics data and push them into the queue for serial processing
     * 当接收到统计数据时 优先考虑批处理 (在缓存中进行整合)
     */
    @Override
    public void in(Metrics metrics) {
        dataCarrier.produce(metrics);
    }

    @Override
    public MergeDataCache<Metrics> getCache() {
        return mergeDataCache;
    }

    /**
     * 将缓存中的数据包装成 req 并传入到列表中
     * @param lastCollection  the source of transformation, they are in memory object format.
     * @param prepareRequests data in the formats for the final persistence operations.
     */
    @Override
    public void prepareBatch(Collection<Metrics> lastCollection, List<PrepareRequest> prepareRequests) {
        long start = System.currentTimeMillis();

        int i = 0;
        int batchGetSize = 2000;
        Metrics[] metrics = null;
        for (Metrics data : lastCollection) {
            // 该对象就是将数据传到一个指定的服务器
            if (Objects.nonNull(nextExportWorker)) {
                // 缺省插件不支持该事件 只有处理 TOTAL 的事件对象  实际上就是一个钩子 通过注入不同的插件实现不同的功能
                ExportEvent event = new ExportEvent(data, ExportEvent.EventType.INCREMENT);
                // 实际上转发到export() 会 NOOP
                nextExportWorker.in(event);
            }
            // 因为 Metrics 具备combine能力 所以不怕数据重复
            if (Objects.nonNull(transWorker)) {
                transWorker.in(data);
            }

            int mod = i % batchGetSize;
            // 小块小块的内存分配 保证系统稳定性
            if (mod == 0) {
                int residual = lastCollection.size() - i;
                if (residual >= batchGetSize) {
                    metrics = new Metrics[batchGetSize];
                } else {
                    metrics = new Metrics[residual];
                }
            }
            // 在对应index 设置数据
            metrics[mod] = data;

            // 当到了数组的最后一个元素时 批量处理并添加到list中
            if (mod == metrics.length - 1) {
                try {
                    // 从数据库中拉一份数据出来
                    syncStorageToCache(metrics);

                    // 最后又做了一次combine
                    for (Metrics metric : metrics) {
                        Metrics cacheMetric = databaseSession.get(metric);
                        if (cacheMetric != null) {
                            cacheMetric.combine(metric);
                            cacheMetric.calculate();
                            prepareRequests.add(metricsDAO.prepareBatchUpdate(model, cacheMetric));
                            nextWorker(cacheMetric);
                        } else {
                            prepareRequests.add(metricsDAO.prepareBatchInsert(model, metric));
                            nextWorker(metric);
                        }
                    }
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                }
            }

            i++;
        }

        if (prepareRequests.size() > 0) {
            log.debug(
                "prepare batch requests for model {}, took time: {}", model.getName(),
                System.currentTimeMillis() - start
            );
        }
    }

    /**
     * 将最终数据交付到 警报模块和 报表模块
     * @param metric
     */
    private void nextWorker(Metrics metric) {
        // 大体的逻辑就是 警报模块有一个规则配置文件 然后每次数据发送过去一旦满足规则就会存在一个window中 之后有个定时器一旦发现在指定时间内有数据 就会触发 RecordStreamProcessor 来保存数据
        // 同时开放一个 webHook 和 GRPCHook 用于自定义补救措施
        if (Objects.nonNull(nextAlarmWorker)) {
            nextAlarmWorker.in(metric);
        }
        // 将数据发送到指定的服务器 (通过GRPC)
        if (Objects.nonNull(nextExportWorker)) {
            ExportEvent event = new ExportEvent(metric, ExportEvent.EventType.TOTAL);
            nextExportWorker.in(event);
        }
    }

    /**
     * 将测量数据先存储在缓存中
     * @param input
     */
    @Override
    public void cacheData(Metrics input) {
        // 内部有2个window 对象 用于读写分离  这里将当前window 标记成writing 状态
        mergeDataCache.writing();
        // 如果当前测量数据已经存在 就进行combine
        if (mergeDataCache.containsKey(input)) {
            Metrics metrics = mergeDataCache.get(input);
            metrics.combine(input);
            metrics.calculate();
        } else {
            // 否则添加映射关系
            input.calculate();
            mergeDataCache.put(input);
        }

        mergeDataCache.finishWriting();
    }

    /**
     * Sync data to the cache if the {@link #enableDatabaseSession} == true.
     * 将数据 与 缓存做同步  这里对db的访问很频繁
     */
    private void syncStorageToCache(Metrics[] metrics) throws IOException {
        if (!enableDatabaseSession) {
            // 该对象内部存放的相当于是 每个统计数据在数据库中的最新值
            databaseSession.clear();
        }

        List<String> notInCacheIds = new ArrayList<>();
        for (Metrics metric : metrics) {
            if (!databaseSession.containsKey(metric)) {
                notInCacheIds.add(metric.id());
            }
        }

        // 如果不主动更新 那么缓存的数据始终是旧的 那维持会话有什么意义呢???  可能作为一个数据统计平台不需要太高的精确性 ???
        if (notInCacheIds.size() > 0) {
            List<Metrics> metricsList = metricsDAO.multiGet(model, notInCacheIds);
            for (Metrics metric : metricsList) {
                databaseSession.put(metric, metric);
            }
        }
    }

    /**
     * 距离上次处理经过了多少时间
     * @param tookTime The time costs in this round.
     */
    @Override
    public void endOfRound(long tookTime) {
        if (enableDatabaseSession) {
            Iterator<Metrics> iterator = databaseSession.values().iterator();
            while (iterator.hasNext()) {
                // 追加存活时间 超过70秒的会移除
                Metrics metrics = iterator.next();
                metrics.extendSurvivalTime(tookTime);
                // 70,000ms means more than one minute.
                if (metrics.getSurvivalTime() > 70000) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Metrics queue processor, merge the received metrics if existing one with same ID(s) and time bucket.
     *
     * ID is declared through {@link IDColumn}
     */
    private class PersistentConsumer implements IConsumer<Metrics> {

        private final MetricsPersistentWorker persistent;

        private PersistentConsumer(MetricsPersistentWorker persistent) {
            this.persistent = persistent;
        }

        @Override
        public void init() {

        }

        @Override
        public void consume(List<Metrics> data) {
            data.forEach(persistent::onWork);
        }

        @Override
        public void onError(List<Metrics> data, Throwable t) {
            log.error(t.getMessage(), t);
        }

        @Override
        public void onExit() {
        }
    }
}
