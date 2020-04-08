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

package org.apache.skywalking.oap.server.core.alarm.provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.alarm.AlarmCallback;
import org.apache.skywalking.oap.server.core.alarm.EndpointMetaInAlarm;
import org.apache.skywalking.oap.server.core.alarm.MetaInAlarm;
import org.apache.skywalking.oap.server.core.alarm.MetricsNotify;
import org.apache.skywalking.oap.server.core.alarm.ServiceInstanceMetaInAlarm;
import org.apache.skywalking.oap.server.core.alarm.ServiceMetaInAlarm;
import org.apache.skywalking.oap.server.core.alarm.provider.grpc.GRPCCallback;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.MetricsMetaInfo;
import org.apache.skywalking.oap.server.core.analysis.metrics.WithMetadata;
import org.apache.skywalking.oap.server.core.cache.EndpointInventoryCache;
import org.apache.skywalking.oap.server.core.cache.ServiceInstanceInventoryCache;
import org.apache.skywalking.oap.server.core.cache.ServiceInventoryCache;
import org.apache.skywalking.oap.server.core.register.EndpointInventory;
import org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory;
import org.apache.skywalking.oap.server.core.register.ServiceInventory;
import org.apache.skywalking.oap.server.core.source.DefaultScopeDefine;
import org.apache.skywalking.oap.server.library.module.ModuleManager;

/**
 * 检测一些警报信息并通知
 */
public class NotifyHandler implements MetricsNotify {
    private ServiceInventoryCache serviceInventoryCache;
    private ServiceInstanceInventoryCache serviceInstanceInventoryCache;
    private EndpointInventoryCache endpointInventoryCache;

    private final AlarmCore core;
    private final AlarmRulesWatcher alarmRulesWatcher;

    public NotifyHandler(AlarmRulesWatcher alarmRulesWatcher) {
        this.alarmRulesWatcher = alarmRulesWatcher;
        core = new AlarmCore(alarmRulesWatcher);
    }

    /**
     * 通过一个定时任务 会定期将保存在缓存中的统计数据发送到这里
     * @param metrics
     */
    @Override
    public void notify(Metrics metrics) {
        WithMetadata withMetadata = (WithMetadata) metrics;
        MetricsMetaInfo meta = withMetadata.getMeta();
        int scope = meta.getScope();

        // 如果数据本身没有携带注解 忽略
        if (!DefaultScopeDefine.inServiceCatalog(scope) && !DefaultScopeDefine.inServiceInstanceCatalog(scope) && !DefaultScopeDefine
            .inEndpointCatalog(scope)) {
            return;
        }

        MetaInAlarm metaInAlarm;
        if (DefaultScopeDefine.inServiceCatalog(scope)) {
            int serviceId = Integer.parseInt(meta.getId());
            ServiceInventory serviceInventory = serviceInventoryCache.get(serviceId);
            ServiceMetaInAlarm serviceMetaInAlarm = new ServiceMetaInAlarm();
            serviceMetaInAlarm.setMetricsName(meta.getMetricsName());
            serviceMetaInAlarm.setId(serviceId);
            serviceMetaInAlarm.setName(serviceInventory.getName());
            metaInAlarm = serviceMetaInAlarm;
        } else if (DefaultScopeDefine.inServiceInstanceCatalog(scope)) {
            int serviceInstanceId = Integer.parseInt(meta.getId());
            ServiceInstanceInventory serviceInstanceInventory = serviceInstanceInventoryCache.get(serviceInstanceId);
            ServiceInstanceMetaInAlarm instanceMetaInAlarm = new ServiceInstanceMetaInAlarm();
            instanceMetaInAlarm.setMetricsName(meta.getMetricsName());
            instanceMetaInAlarm.setId(serviceInstanceId);
            instanceMetaInAlarm.setName(serviceInstanceInventory.getName());
            metaInAlarm = instanceMetaInAlarm;
        } else if (DefaultScopeDefine.inEndpointCatalog(scope)) {
            int endpointId = Integer.parseInt(meta.getId());
            EndpointInventory endpointInventory = endpointInventoryCache.get(endpointId);
            EndpointMetaInAlarm endpointMetaInAlarm = new EndpointMetaInAlarm();
            endpointMetaInAlarm.setMetricsName(meta.getMetricsName());
            endpointMetaInAlarm.setId(endpointId);

            int serviceId = endpointInventory.getServiceId();
            ServiceInventory serviceInventory = serviceInventoryCache.get(serviceId);

            String textName = endpointInventory.getName() + " in " + serviceInventory.getName();

            endpointMetaInAlarm.setName(textName);
            metaInAlarm = endpointMetaInAlarm;
        } else {
            return;
        }

        // 找到数据时先判断有没有设置该数据项对应的 警报策略
        List<RunningRule> runningRules = core.findRunningRule(meta.getMetricsName());
        if (runningRules == null) {
            return;
        }

        runningRules.forEach(rule -> rule.in(metaInAlarm, metrics));
    }

    /**
     * 为该对象设置一个回调对象 当接收到警报信息时用该对象来处理
     * @param callbacks
     */
    public void init(AlarmCallback... callbacks) {
        List<AlarmCallback> allCallbacks = new ArrayList<>(Arrays.asList(callbacks));
        allCallbacks.add(new WebhookCallback(alarmRulesWatcher));
        allCallbacks.add(new GRPCCallback(alarmRulesWatcher));
        core.start(allCallbacks);
    }

    public void initCache(ModuleManager moduleManager) {
        serviceInventoryCache = moduleManager.find(CoreModule.NAME).provider().getService(ServiceInventoryCache.class);
        serviceInstanceInventoryCache = moduleManager.find(CoreModule.NAME)
                                                     .provider()
                                                     .getService(ServiceInstanceInventoryCache.class);
        endpointInventoryCache = moduleManager.find(CoreModule.NAME)
                                              .provider()
                                              .getService(EndpointInventoryCache.class);
    }
}
