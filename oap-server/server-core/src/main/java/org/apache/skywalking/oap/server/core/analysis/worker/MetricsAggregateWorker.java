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

import java.util.Iterator;
import java.util.List;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.consumer.BulkConsumePool;
import org.apache.skywalking.apm.commons.datacarrier.consumer.ConsumerPoolFactory;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.data.MergeDataCache;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetricsAggregateWorker provides an in-memory metrics merging capability. This aggregation is called L1 aggregation,
 * it merges the data just after the receiver analysis. The metrics belonging to the same entity, metrics type and time
 * bucket, the L1 aggregation will merge them into one metrics object to reduce the unnecessary memory and network
 * payload.
 * 该对象是一个包装对象 应该是为了实现批处理的功能
 */
public class MetricsAggregateWorker extends AbstractWorker<Metrics> {

    private static final Logger logger = LoggerFactory.getLogger(MetricsAggregateWorker.class);

    private AbstractWorker<Metrics> nextWorker;
    /**
     * 生产者-消费者池
     */
    private final DataCarrier<Metrics> dataCarrier;
    private final MergeDataCache<Metrics> mergeDataCache;
    private CounterMetrics aggregationCounter;

    MetricsAggregateWorker(ModuleDefineHolder moduleDefineHolder, AbstractWorker<Metrics> nextWorker,
                           String modelName) {
        super(moduleDefineHolder);
        this.nextWorker = nextWorker;
        this.mergeDataCache = new MergeDataCache<>();
        String name = "METRICS_L1_AGGREGATION";
        this.dataCarrier = new DataCarrier<>("MetricsAggregateWorker." + modelName, name, 2, 10000);

        BulkConsumePool.Creator creator = new BulkConsumePool.Creator(
            name, BulkConsumePool.Creator.recommendMaxSize() * 2, 20);
        try {
            ConsumerPoolFactory.INSTANCE.createIfAbsent(name, creator);
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage(), e);
        }
        this.dataCarrier.consume(ConsumerPoolFactory.INSTANCE.get(name), new AggregatorConsumer(this));

        MetricsCreator metricsCreator = moduleDefineHolder.find(TelemetryModule.NAME)
                                                          .provider()
                                                          .getService(MetricsCreator.class);
        aggregationCounter = metricsCreator.createCounter(
            "metrics_aggregation", "The number of rows in aggregation",
            new MetricsTag.Keys("metricName", "level", "dimensionality"), new MetricsTag.Values(modelName, "1", "min")
        );
    }

    @Override
    public final void in(Metrics metrics) {
        metrics.resetEndOfBatch();
        dataCarrier.produce(metrics);
    }

    /**
     * 处理生产者生成的数据
     * @param metrics
     */
    private void onWork(Metrics metrics) {
        aggregationCounter.inc();
        aggregate(metrics);

        if (metrics.isEndOfBatch()) {
            sendToNext();
        }
    }

    /**
     * 每批任务执行到最后时 需要触发该方法
     */
    private void sendToNext() {
        // 切换容器同时将上个容器标记成写状态
        mergeDataCache.switchPointer();
        while (mergeDataCache.getLast().isWriting()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        mergeDataCache.getLast().collection().forEach(data -> {
            if (logger.isDebugEnabled()) {
                logger.debug(data.toString());
            }

            nextWorker.in(data);
        });
        // 当读取完成时 修改标识 同时清除旧数据  这样就确保在读取某个容器时 数据写入新容器 从而避免频繁的读写冲突   相当于并发点仅在一个volatile标识修饰变量
        mergeDataCache.finishReadingLast();
    }

    /**
     * 将当前统计的数据尽可能聚合  应该是针对内存的写入 速度会比database network 这些要快很多 所以尽可能的采用批处理
     * @param metrics
     */
    private void aggregate(Metrics metrics) {
        mergeDataCache.writing();
        if (mergeDataCache.containsKey(metrics)) {
            mergeDataCache.get(metrics).combine(metrics);
        } else {
            mergeDataCache.put(metrics);
        }

        mergeDataCache.finishWriting();
    }

    private class AggregatorConsumer implements IConsumer<Metrics> {

        private final MetricsAggregateWorker aggregator;

        private AggregatorConsumer(MetricsAggregateWorker aggregator) {
            this.aggregator = aggregator;
        }

        @Override
        public void init() {

        }

        @Override
        public void consume(List<Metrics> data) {
            Iterator<Metrics> inputIterator = data.iterator();

            int i = 0;
            while (inputIterator.hasNext()) {
                Metrics metrics = inputIterator.next();
                i++;
                if (i == data.size()) {
                    metrics.asEndOfBatch();
                }
                aggregator.onWork(metrics);
            }
        }

        @Override
        public void onError(List<Metrics> data, Throwable t) {
            logger.error(t.getMessage(), t);
        }

        @Override
        public void onExit() {
        }
    }
}
