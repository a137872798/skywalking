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

package org.apache.skywalking.oap.server.receiver.trace.provider;

import org.apache.skywalking.oap.server.configuration.api.ConfigurationModule;
import org.apache.skywalking.oap.server.configuration.api.DynamicConfigurationService;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.server.GRPCHandlerRegister;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;
import org.apache.skywalking.oap.server.library.module.ModuleDefine;
import org.apache.skywalking.oap.server.library.module.ModuleProvider;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.module.ServiceNotProvidedException;
import org.apache.skywalking.oap.server.receiver.sharing.server.SharingServerModule;
import org.apache.skywalking.oap.server.receiver.trace.module.TraceModule;
import org.apache.skywalking.oap.server.receiver.trace.provider.handler.v6.grpc.TraceSegmentReportServiceHandler;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.ISegmentParserService;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.SegmentParseV2;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.SegmentParserListenerManager;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.SegmentParserServiceImpl;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.endpoint.MultiScopesSpanListener;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.segment.SegmentSpanListener;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.service.ServiceInstanceMappingSpanListener;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.listener.service.ServiceMappingSpanListener;
import org.apache.skywalking.oap.server.receiver.trace.provider.parser.standardization.SegmentStandardizationWorker;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;

import java.io.IOException;

/**
 * 当依赖中包含  TraceModule 时 自动配置该provider 并进行数据加载
 */
public class TraceModuleProvider extends ModuleProvider {

    /**
     * 有关链路追踪的配置
     */
    private final TraceServiceModuleConfig moduleConfig;
    private SegmentParseV2.Producer segmentProducerV2;
    // 网关和数据库的配置
    private DBLatencyThresholdsAndWatcher thresholds;
    private UninstrumentedGatewaysConfig uninstrumentedGatewaysConfig;

    public TraceModuleProvider() {
        this.moduleConfig = new TraceServiceModuleConfig();
    }

    @Override
    public String name() {
        return "default";
    }

    @Override
    public Class<? extends ModuleDefine> module() {
        return TraceModule.class;
    }

    @Override
    public ModuleConfig createConfigBeanIfAbsent() {
        return moduleConfig;
    }

    @Override
    public void prepare() throws ServiceNotProvidedException {
        thresholds = new DBLatencyThresholdsAndWatcher(moduleConfig.getSlowDBAccessThreshold(), this);

        // 网关配置
        uninstrumentedGatewaysConfig = new UninstrumentedGatewaysConfig(this);

        moduleConfig.setDbLatencyThresholdsAndWatcher(thresholds);
        moduleConfig.setUninstrumentedGatewaysConfig(uninstrumentedGatewaysConfig);

        segmentProducerV2 = new SegmentParseV2.Producer(getManager(), listenerManager(), moduleConfig);

        this.registerServiceImplementation(ISegmentParserService.class, new SegmentParserServiceImpl(segmentProducerV2));
    }

    /**
     * 获取segment解析监听器管理者
     * @return
     */
    public SegmentParserListenerManager listenerManager() {
        SegmentParserListenerManager listenerManager = new SegmentParserListenerManager();
        // 是否要分析trace信息  要的话就添加几个特殊的工厂
        if (moduleConfig.isTraceAnalysis()) {
            listenerManager.add(new MultiScopesSpanListener.Factory());
            listenerManager.add(new ServiceMappingSpanListener.Factory());
            listenerManager.add(new ServiceInstanceMappingSpanListener.Factory());
        }
        listenerManager.add(new SegmentSpanListener.Factory(moduleConfig.getSampleRate()));

        return listenerManager;
    }

    /**
     * 启动链路分析模块
     * @throws ModuleStartException
     */
    @Override
    public void start() throws ModuleStartException {
        DynamicConfigurationService dynamicConfigurationService = getManager().find(ConfigurationModule.NAME)
                                                                              .provider()
                                                                              .getService(DynamicConfigurationService.class);
        GRPCHandlerRegister grpcHandlerRegister = getManager().find(SharingServerModule.NAME)
                                                              .provider()
                                                              .getService(GRPCHandlerRegister.class);
        try {
            // 将2个监听动态配置的 watcher 设置到注册中心
            dynamicConfigurationService.registerConfigChangeWatcher(thresholds);
            dynamicConfigurationService.registerConfigChangeWatcher(uninstrumentedGatewaysConfig);

            // 设置处理器 这样对端调用存根方法时就会触发handler的方法
            grpcHandlerRegister.addHandler(new TraceSegmentReportServiceHandler(segmentProducerV2, getManager()));

            // 设置worker 对象
            SegmentStandardizationWorker standardizationWorkerV2 = new SegmentStandardizationWorker(getManager(), segmentProducerV2, moduleConfig
                .getBufferPath(), moduleConfig.getBufferOffsetMaxFileSize(), moduleConfig.getBufferDataMaxFileSize(), moduleConfig
                .isBufferFileCleanWhenRestart());
            segmentProducerV2.setStandardizationWorker(standardizationWorkerV2);
        } catch (IOException e) {
            throw new ModuleStartException(e.getMessage(), e);
        }
    }

    @Override
    public void notifyAfterCompleted() {

    }

    @Override
    public String[] requiredModules() {
        return new String[] {
            TelemetryModule.NAME,
            CoreModule.NAME,
            SharingServerModule.NAME,   // 该模块就是启动 jetty 和 grpc  并且提供2个可以给服务器注册handler 的service
            ConfigurationModule.NAME
        };
    }
}
