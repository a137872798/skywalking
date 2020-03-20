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

package org.apache.skywalking.oap.server.exporter.provider.grpc;

import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.cache.EndpointInventoryCache;
import org.apache.skywalking.oap.server.core.cache.ServiceInstanceInventoryCache;
import org.apache.skywalking.oap.server.core.cache.ServiceInventoryCache;
import org.apache.skywalking.oap.server.core.exporter.ExporterModule;
import org.apache.skywalking.oap.server.core.exporter.MetricValuesExportService;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;
import org.apache.skywalking.oap.server.library.module.ModuleDefine;
import org.apache.skywalking.oap.server.library.module.ModuleProvider;
import org.apache.skywalking.oap.server.library.module.ModuleServiceHolder;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.module.ServiceNotProvidedException;

public class GRPCExporterProvider extends ModuleProvider {
    private GRPCExporterSetting setting;
    private GRPCExporter exporter;

    @Override
    public String name() {
        return "grpc";
    }

    /**
     * 注意针对某个 module 一次只能使用一个 provider
     * @return
     */
    @Override
    public Class<? extends ModuleDefine> module() {
        return ExporterModule.class;
    }

    @Override
    public ModuleConfig createConfigBeanIfAbsent() {
        setting = new GRPCExporterSetting();
        return setting;
    }

    /**
     * 当从 ApplicationConfiguration 读取该模块相关的配置后
     * @throws ServiceNotProvidedException
     * @throws ModuleStartException
     */
    @Override
    public void prepare() throws ServiceNotProvidedException, ModuleStartException {
        // 根据配置生成 service 并注册到provider 上  这样外部就可以通过 ModuleManager 间接找到这个Service 并调用相关方法
        exporter = new GRPCExporter(setting);
        this.registerServiceImplementation(MetricValuesExportService.class, exporter);
    }

    @Override
    public void start() throws ServiceNotProvidedException, ModuleStartException {

    }

    @Override
    public void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {
        ModuleServiceHolder serviceHolder = getManager().find(CoreModule.NAME).provider();
        exporter.setServiceInventoryCache(serviceHolder.getService(ServiceInventoryCache.class));
        exporter.setServiceInstanceInventoryCache(serviceHolder.getService(ServiceInstanceInventoryCache.class));
        exporter.setEndpointInventoryCache(serviceHolder.getService(EndpointInventoryCache.class));

        exporter.initSubscriptionList();
    }

    @Override
    public String[] requiredModules() {
        return new String[] {CoreModule.NAME};
    }
}
