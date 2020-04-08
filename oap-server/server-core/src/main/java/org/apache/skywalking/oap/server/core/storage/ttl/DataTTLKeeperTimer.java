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

package org.apache.skywalking.oap.server.core.storage.ttl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.CoreModuleConfig;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.cluster.ClusterModule;
import org.apache.skywalking.oap.server.core.cluster.ClusterNodesQuery;
import org.apache.skywalking.oap.server.core.cluster.RemoteInstance;
import org.apache.skywalking.oap.server.core.storage.IHistoryDeleteDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.model.IModelGetter;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TTL = Time To Live
 *
 * DataTTLKeeperTimer is an internal timer, it drives the {@link IHistoryDeleteDAO} to remove the expired data. TTL
 * configurations are provided in {@link CoreModuleConfig}, some storage implementations, such as ES6/ES7, provides an
 * override TTL, which could be more suitable for the implementation. No matter which TTL configurations are set, they
 * are all driven by this timer.
 */
public enum DataTTLKeeperTimer {
    INSTANCE;

    private static final Logger logger = LoggerFactory.getLogger(DataTTLKeeperTimer.class);

    private ModuleManager moduleManager;
    private ClusterNodesQuery clusterNodesQuery;

    public void start(ModuleManager moduleManager, CoreModuleConfig moduleConfig) {
        this.moduleManager = moduleManager;
        this.clusterNodesQuery = moduleManager.find(ClusterModule.NAME).provider().getService(ClusterNodesQuery.class);

        Executors.newSingleThreadScheduledExecutor()
                 .scheduleAtFixedRate(
                     new RunnableWithExceptionProtection(
                         this::delete,
                         t -> logger.error("Remove data in background failure.", t)
                     ), moduleConfig
                         .getDataKeeperExecutePeriod(), moduleConfig.getDataKeeperExecutePeriod(), TimeUnit.MINUTES);
    }

    /**
     * DataTTLKeeperTimer starts in every OAP node, but the deletion only work when it is as the first node in the OAP
     * node list from {@link ClusterNodesQuery}.
     */
    private void delete() {
        // 每隔一段时间获取当前集群中所有节点信息
        List<RemoteInstance> remoteInstances = clusterNodesQuery.queryRemoteNodes();
        // 集群中的首节点是不删除的 同时一般来说会选择 foreverFirst策略 使得所有数据都会发往一个节点进行保存  那么也就是大部分数据实际上还是不会被删除 那么这个有啥用
        if (CollectionUtils.isNotEmpty(remoteInstances) && !remoteInstances.get(0).getAddress().isSelf()) {
            logger.info("The selected first getAddress is {}. Skip.", remoteInstances.get(0).toString());
            return;
        }

        logger.info("Beginning to remove expired metrics from the storage.");
        IModelGetter modelGetter = moduleManager.find(CoreModule.NAME).provider().getService(IModelGetter.class);
        List<Model> models = modelGetter.getModels();
        models.forEach(model -> {
            if (model.isDeleteHistory()) {
                execute(model);
            }
        });
    }

    private void execute(Model model) {
        try {
            moduleManager.find(StorageModule.NAME)
                         .provider()
                         .getService(IHistoryDeleteDAO.class)
                         .deleteHistory(model, Metrics.TIME_BUCKET);
        } catch (IOException e) {
            logger.warn("History of {} delete failure", model.getName());
            logger.error(e.getMessage(), e);
        }
    }
}