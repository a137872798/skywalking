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

package org.apache.skywalking.oap.server.core.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.oap.server.core.CoreModuleConfig;
import org.apache.skywalking.oap.server.core.analysis.worker.MetricsStreamProcessor;
import org.apache.skywalking.oap.server.core.analysis.worker.PersistenceWorker;
import org.apache.skywalking.oap.server.core.analysis.worker.TopNStreamProcessor;
import org.apache.skywalking.oap.server.library.client.request.PrepareRequest;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.HistogramMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum PersistenceTimer {
    INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(PersistenceTimer.class);

    private Boolean isStarted = false;
    private final Boolean debug;
    // 通过遥感模块 创建各个统计数据项
    private CounterMetrics errorCounter;
    private HistogramMetrics prepareLatency;
    private HistogramMetrics executeLatency;
    private long lastTime = System.currentTimeMillis();
    /**
     * 存储待执行的sql
     */
    private final List<PrepareRequest> prepareRequests = new ArrayList<>(50000);

    PersistenceTimer() {
        this.debug = System.getProperty("debug") != null;
    }

    /**
     * 启动持久化扫描对象
     * @param moduleManager
     * @param moduleConfig
     */
    public void start(ModuleManager moduleManager, CoreModuleConfig moduleConfig) {
        logger.info("persistence timer start");
        // 批存储对象 就是预先封装好预备执行的sql param 之后在合适的时机手动触发 批量执行
        IBatchDAO batchDAO = moduleManager.find(StorageModule.NAME).provider().getService(IBatchDAO.class);

        MetricsCreator metricsCreator = moduleManager.find(TelemetryModule.NAME)
                                                     .provider()
                                                     .getService(MetricsCreator.class);
        errorCounter = metricsCreator.createCounter("persistence_timer_bulk_error_count", "Error execution of the prepare stage in persistence timer", MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);
        prepareLatency = metricsCreator.createHistogramMetric("persistence_timer_bulk_prepare_latency", "Latency of the prepare stage in persistence timer", MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);
        executeLatency = metricsCreator.createHistogramMetric("persistence_timer_bulk_execute_latency", "Latency of the execute stage in persistence timer", MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);

        // 这个标识根本没意义啊 如果不是并发场景 人为可控的话 只要调用一次start就好 如果在并发场景这个标识又不满足可见性
        if (!isStarted) {
            Executors.newSingleThreadScheduledExecutor()
                     .scheduleWithFixedDelay(new RunnableWithExceptionProtection(() -> extractDataAndSave(batchDAO), t -> logger
                         .error("Extract data and save failure.", t)), 5, moduleConfig.getPersistentPeriod(), TimeUnit.SECONDS);

            this.isStarted = true;
        }
    }

    /**
     * 定时持久化数据
     * @param batchDAO
     */
    private void extractDataAndSave(IBatchDAO batchDAO) {
        if (logger.isDebugEnabled()) {
            logger.debug("Extract data and save");
        }

        long startTime = System.currentTimeMillis();

        try {
            HistogramMetrics.Timer timer = prepareLatency.createTimer();

            try {
                // 将当前已经存储的所有worker 全部转移进来
                List<PersistenceWorker> persistenceWorkers = new ArrayList<>();
                persistenceWorkers.addAll(TopNStreamProcessor.getInstance().getPersistentWorkers());
                persistenceWorkers.addAll(MetricsStreamProcessor.getInstance().getPersistentWorkers());

                persistenceWorkers.forEach(worker -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("extract {} worker data and save", worker.getClass().getName());
                    }

                    // 这里做读写分离
                    if (worker.flushAndSwitch()) {
                        // 将worker 中的数据转换成sql 并添加到 req列表中
                        worker.buildBatchRequests(prepareRequests);
                    }

                    // databaseSession 相关   TopN数据忽略该方法
                    worker.endOfRound(System.currentTimeMillis() - lastTime);
                });

                if (debug) {
                    logger.info("build batch persistence duration: {} ms", System.currentTimeMillis() - startTime);
                }
            } finally {
                timer.finish();
            }

            HistogramMetrics.Timer executeLatencyTimer = executeLatency.createTimer();
            try {
                // 挨个执行
                if (CollectionUtils.isNotEmpty(prepareRequests)) {
                    batchDAO.synchronous(prepareRequests);
                }
            } finally {
                executeLatencyTimer.finish();
            }
        } catch (Throwable e) {
            errorCounter.inc();
            logger.error(e.getMessage(), e);
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Persistence data save finish");
            }

            prepareRequests.clear();
            lastTime = System.currentTimeMillis();
        }

        if (debug) {
            logger.info("Batch persistence duration: {} ms", System.currentTimeMillis() - startTime);
        }
    }
}
