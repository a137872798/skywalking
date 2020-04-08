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

package org.apache.skywalking.oap.server.core;

import java.io.IOException;
import org.apache.skywalking.oap.server.configuration.api.ConfigurationModule;
import org.apache.skywalking.oap.server.configuration.api.DynamicConfigurationService;
import org.apache.skywalking.oap.server.core.analysis.ApdexThresholdConfig;
import org.apache.skywalking.oap.server.core.analysis.DisableRegister;
import org.apache.skywalking.oap.server.core.analysis.StreamAnnotationListener;
import org.apache.skywalking.oap.server.core.analysis.metrics.ApdexMetrics;
import org.apache.skywalking.oap.server.core.analysis.worker.MetricsStreamProcessor;
import org.apache.skywalking.oap.server.core.analysis.worker.TopNStreamProcessor;
import org.apache.skywalking.oap.server.core.annotation.AnnotationScan;
import org.apache.skywalking.oap.server.core.cache.CacheUpdateTimer;
import org.apache.skywalking.oap.server.core.cache.EndpointInventoryCache;
import org.apache.skywalking.oap.server.core.cache.NetworkAddressInventoryCache;
import org.apache.skywalking.oap.server.core.cache.ProfileTaskCache;
import org.apache.skywalking.oap.server.core.cache.ServiceInstanceInventoryCache;
import org.apache.skywalking.oap.server.core.cache.ServiceInventoryCache;
import org.apache.skywalking.oap.server.core.cluster.ClusterModule;
import org.apache.skywalking.oap.server.core.cluster.ClusterRegister;
import org.apache.skywalking.oap.server.core.cluster.RemoteInstance;
import org.apache.skywalking.oap.server.core.command.CommandService;
import org.apache.skywalking.oap.server.core.config.ComponentLibraryCatalogService;
import org.apache.skywalking.oap.server.core.config.ConfigService;
import org.apache.skywalking.oap.server.core.config.DownsamplingConfigService;
import org.apache.skywalking.oap.server.core.config.IComponentLibraryCatalogService;
import org.apache.skywalking.oap.server.core.oal.rt.OALEngine;
import org.apache.skywalking.oap.server.core.oal.rt.OALEngineLoader;
import org.apache.skywalking.oap.server.core.profile.ProfileTaskMutationService;
import org.apache.skywalking.oap.server.core.query.AggregationQueryService;
import org.apache.skywalking.oap.server.core.query.AlarmQueryService;
import org.apache.skywalking.oap.server.core.query.LogQueryService;
import org.apache.skywalking.oap.server.core.query.MetadataQueryService;
import org.apache.skywalking.oap.server.core.query.MetricQueryService;
import org.apache.skywalking.oap.server.core.query.ProfileTaskQueryService;
import org.apache.skywalking.oap.server.core.query.TopNRecordsQueryService;
import org.apache.skywalking.oap.server.core.query.TopologyQueryService;
import org.apache.skywalking.oap.server.core.query.TraceQueryService;
import org.apache.skywalking.oap.server.core.register.service.EndpointInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.IEndpointInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.INetworkAddressInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.IServiceInstanceInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.IServiceInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.NetworkAddressInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.ServiceInstanceInventoryRegister;
import org.apache.skywalking.oap.server.core.register.service.ServiceInventoryRegister;
import org.apache.skywalking.oap.server.core.remote.RemoteSenderService;
import org.apache.skywalking.oap.server.core.remote.RemoteServiceHandler;
import org.apache.skywalking.oap.server.core.remote.client.Address;
import org.apache.skywalking.oap.server.core.remote.client.RemoteClientManager;
import org.apache.skywalking.oap.server.core.remote.health.HealthCheckServiceHandler;
import org.apache.skywalking.oap.server.core.server.GRPCHandlerRegister;
import org.apache.skywalking.oap.server.core.server.GRPCHandlerRegisterImpl;
import org.apache.skywalking.oap.server.core.server.JettyHandlerRegister;
import org.apache.skywalking.oap.server.core.server.JettyHandlerRegisterImpl;
import org.apache.skywalking.oap.server.core.source.DefaultScopeDefine;
import org.apache.skywalking.oap.server.core.source.SourceReceiver;
import org.apache.skywalking.oap.server.core.source.SourceReceiverImpl;
import org.apache.skywalking.oap.server.core.storage.PersistenceTimer;
import org.apache.skywalking.oap.server.core.storage.model.IModelGetter;
import org.apache.skywalking.oap.server.core.storage.model.IModelOverride;
import org.apache.skywalking.oap.server.core.storage.model.IModelSetter;
import org.apache.skywalking.oap.server.core.storage.model.StorageModels;
import org.apache.skywalking.oap.server.core.storage.ttl.DataTTLKeeperTimer;
import org.apache.skywalking.oap.server.core.worker.IWorkerInstanceGetter;
import org.apache.skywalking.oap.server.core.worker.IWorkerInstanceSetter;
import org.apache.skywalking.oap.server.core.worker.WorkerInstancesService;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;
import org.apache.skywalking.oap.server.library.module.ModuleDefine;
import org.apache.skywalking.oap.server.library.module.ModuleProvider;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.module.ServiceNotProvidedException;
import org.apache.skywalking.oap.server.library.server.ServerException;
import org.apache.skywalking.oap.server.library.server.grpc.GRPCServer;
import org.apache.skywalking.oap.server.library.server.jetty.JettyServer;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;

/**
 * Core module provider includes the recommended and default implementations of {@link CoreModule#services()}. All
 * services with these default implementations are widely used including data receiver, data analysis, streaming
 * process, storage and query.
 *
 * NOTICE. In our experiences, no one should re-implement the core module service implementations, unless we are very
 * familiar with all mechanisms of SkyWalking.
 * 启动一些核心service  并且很多服务都需要依赖CoreModule
 */
public class CoreModuleProvider extends ModuleProvider {

    private final CoreModuleConfig moduleConfig;
    private GRPCServer grpcServer;
    private JettyServer jettyServer;
    private RemoteClientManager remoteClientManager;
    private final AnnotationScan annotationScan;
    private final StorageModels storageModels;
    private final SourceReceiverImpl receiver;
    private OALEngine oalEngine;
    private ApdexThresholdConfig apdexThresholdConfig;

    /**
     * 一个  provider 的启动流程是  先通过SPI 机制进行初始化
     */
    public CoreModuleProvider() {
        super();
        // 该对象内部包含了需要的参数
        this.moduleConfig = new CoreModuleConfig();
        // 创建注解扫描器  这里还没有添加关注的注解以及监听器
        this.annotationScan = new AnnotationScan();
        // 数据实体仓库 将携带 @Stream的类信息抽取出来
        this.storageModels = new StorageModels();
        // 数据接收器  该对象内部有一个 DispatcherManager 作为分发的入口 然后将不同的source 按照scope 转发到不同的dispatcher
        this.receiver = new SourceReceiverImpl();
    }

    @Override
    public String name() {
        return "default";
    }

    @Override
    public Class<? extends ModuleDefine> module() {
        return CoreModule.class;
    }

    /**
     * 第二步 创建配置 并且将配置文件的数据注入
     * @return
     */
    @Override
    public ModuleConfig createConfigBeanIfAbsent() {
        return moduleConfig;
    }

    /**
     * 第三步调用该方法 通过填充完的 config对象去创建 service
     * @throws ServiceNotProvidedException
     * @throws ModuleStartException
     */
    @Override
    public void prepare() throws ServiceNotProvidedException, ModuleStartException {
        if (moduleConfig.isActiveExtraModelColumns()) {
            // 设置全局标识
            DefaultScopeDefine.activeExtraModelColumns();
        }

        // getManager 返回ModuleManager
        // StreamAnnotationListener 可以理解为一个model 探测器  将model 与worker的映射关系添加到 streamProcessor 这样不同类型的model数据就会走不同的处理逻辑
        StreamAnnotationListener streamAnnotationListener = new StreamAnnotationListener(getManager());

        // 该对象专门负责扫描注解
        AnnotationScan scopeScan = new AnnotationScan();
        scopeScan.registerListener(new DefaultScopeDefine.Listener());
        try {
            // 扫描并触发监听器
            scopeScan.scan();

            // 这里初始化 oalEngine 在这里进行初始化是因为 该对象依赖于 Core
            oalEngine = OALEngineLoader.get();
            oalEngine.setStreamListener(streamAnnotationListener);
            oalEngine.setDispatcherListener(receiver.getDispatcherManager());
            // 根据脚本信息 动态生成一些跟 内置model类似的类  如果使用现成的测量项来展示应该就不需要生成动态类了
            oalEngine.start(getClass().getClassLoader());
        } catch (Exception e) {
            throw new ModuleStartException(e.getMessage(), e);
        }

        // 监听 @Disable 相关的注解
        AnnotationScan oalDisable = new AnnotationScan();
        oalDisable.registerListener(DisableRegister.INSTANCE);
        oalDisable.registerListener(new DisableRegister.SingleDisableScanListener());
        try {
            oalDisable.scan();
        } catch (IOException e) {
            throw new ModuleStartException(e.getMessage(), e);
        }

        // 每个启动 oal的 节点都会将自身作为一个 server  这里不细看了
        grpcServer = new GRPCServer(moduleConfig.getGRPCHost(), moduleConfig.getGRPCPort());
        if (moduleConfig.getMaxConcurrentCallsPerConnection() > 0) {
            grpcServer.setMaxConcurrentCallsPerConnection(moduleConfig.getMaxConcurrentCallsPerConnection());
        }
        if (moduleConfig.getMaxMessageSize() > 0) {
            grpcServer.setMaxMessageSize(moduleConfig.getMaxMessageSize());
        }
        if (moduleConfig.getGRPCThreadPoolQueueSize() > 0) {
            grpcServer.setThreadPoolQueueSize(moduleConfig.getGRPCThreadPoolQueueSize());
        }
        if (moduleConfig.getGRPCThreadPoolSize() > 0) {
            grpcServer.setThreadPoolSize(moduleConfig.getGRPCThreadPoolSize());
        }
        grpcServer.initialize();

        // 启动一个 jetty 服务器
        jettyServer = new JettyServer(
            moduleConfig.getRestHost(), moduleConfig.getRestPort(), moduleConfig.getRestContextPath(), moduleConfig
            .getJettySelectors());
        jettyServer.initialize();

        // 注册核心服务
        // 包含GRPC 配置
        this.registerServiceImplementation(ConfigService.class, new ConfigService(moduleConfig));
        // 采样相关配置 按照几个维度  配合 MetricStreamProcessor 使用
        this.registerServiceImplementation(
            DownsamplingConfigService.class, new DownsamplingConfigService(moduleConfig.getDownsampling()));

        // 下面2个都是特定某个框架 先跳过
        this.registerServiceImplementation(GRPCHandlerRegister.class, new GRPCHandlerRegisterImpl(grpcServer));
        this.registerServiceImplementation(JettyHandlerRegister.class, new JettyHandlerRegisterImpl(jettyServer));

        // 组件目录 通过读取 component-libraries.yml 的映射关系生成
        this.registerServiceImplementation(IComponentLibraryCatalogService.class, new ComponentLibraryCatalogService());

        // 该对象会将接收到的数据转发到 SourceDispatcher 实现类
        this.registerServiceImplementation(SourceReceiver.class, receiver);

        // 接收到数据流后 往集群中某个节点发送 如果发送到本机时 则使用该对象内部的worker 来处理数据
        WorkerInstancesService instancesService = new WorkerInstancesService();
        this.registerServiceImplementation(IWorkerInstanceGetter.class, instancesService);
        this.registerServiceImplementation(IWorkerInstanceSetter.class, instancesService);

        // 从一组client 中选择一个并传播数据
        this.registerServiceImplementation(RemoteSenderService.class, new RemoteSenderService(getManager()));
        // 统计项模型管理器
        this.registerServiceImplementation(IModelSetter.class, storageModels);
        this.registerServiceImplementation(IModelGetter.class, storageModels);
        this.registerServiceImplementation(IModelOverride.class, storageModels);

        this.registerServiceImplementation(
            ServiceInventoryCache.class, new ServiceInventoryCache(getManager(), moduleConfig));
        // 该对象用于注册服务实例 通过StreamProcessor 后会经由 worker 进行持久化
        this.registerServiceImplementation(IServiceInventoryRegister.class, new ServiceInventoryRegister(getManager()));

        // 与上面类似
        this.registerServiceImplementation(
            ServiceInstanceInventoryCache.class, new ServiceInstanceInventoryCache(getManager(), moduleConfig));
        this.registerServiceImplementation(
            IServiceInstanceInventoryRegister.class, new ServiceInstanceInventoryRegister(getManager()));

        // 数据体缓存以及 注册对象 与上面的套路一致
        this.registerServiceImplementation(
            EndpointInventoryCache.class, new EndpointInventoryCache(getManager(), moduleConfig));
        this.registerServiceImplementation(
            IEndpointInventoryRegister.class, new EndpointInventoryRegister(getManager()));

        // 存储网络信息实体
        this.registerServiceImplementation(
            NetworkAddressInventoryCache.class, new NetworkAddressInventoryCache(getManager(), moduleConfig));
        this.registerServiceImplementation(
            INetworkAddressInventoryRegister.class, new NetworkAddressInventoryRegister(getManager()));

        // 当使用者调用 register注册服务信息时 会间接触发 StreamProcess 将注册数据持久化 之后下次在调用register获取信息时 就可以通过Cache 服务直接读取上次存储的数据  //


        // 下面对应的都是一些查询服务  需要配合其他组件保存数据
        this.registerServiceImplementation(TopologyQueryService.class, new TopologyQueryService(getManager()));
        this.registerServiceImplementation(MetricQueryService.class, new MetricQueryService(getManager()));
        this.registerServiceImplementation(TraceQueryService.class, new TraceQueryService(getManager()));
        this.registerServiceImplementation(LogQueryService.class, new LogQueryService(getManager()));
        this.registerServiceImplementation(MetadataQueryService.class, new MetadataQueryService(getManager()));
        this.registerServiceImplementation(AggregationQueryService.class, new AggregationQueryService(getManager()));
        this.registerServiceImplementation(AlarmQueryService.class, new AlarmQueryService(getManager()));
        this.registerServiceImplementation(TopNRecordsQueryService.class, new TopNRecordsQueryService(getManager()));

        // add profile service implementations
        // 该对象负责生成profileTask
        this.registerServiceImplementation(
            ProfileTaskMutationService.class, new ProfileTaskMutationService(getManager()));
        // 负责查询之前插入的数据
        this.registerServiceImplementation(
            ProfileTaskQueryService.class, new ProfileTaskQueryService(getManager(), moduleConfig));
        // 提供一层缓存层  这里好像没有做数据库,缓存双写一致
        this.registerServiceImplementation(ProfileTaskCache.class, new ProfileTaskCache(getManager(), moduleConfig));

        // 通过该对象可以快捷的创建一些 命令对象
        this.registerServiceImplementation(CommandService.class, new CommandService(getManager()));

        // 这里注册监听@Stream 注解的listener
        annotationScan.registerListener(streamAnnotationListener);

        // 该对象配合配置中心拉取当前集群所有节点 并建立长连接
        this.remoteClientManager = new RemoteClientManager(getManager(), moduleConfig.getRemoteTimeout());
        this.registerServiceImplementation(RemoteClientManager.class, remoteClientManager);

        // 下面的2个属性都是要配合 定时器的  因为这2个stream接收到数据后只是简单的将数据存储到缓存中   通过后台定时器去处理缓存中的数据 比如持久化 或者做运算之类的
        MetricsStreamProcessor.getInstance().setEnableDatabaseSession(moduleConfig.isEnableDatabaseSession());
        // 设置报告间隔
        TopNStreamProcessor.getInstance().setTopNWorkerReportCycle(moduleConfig.getTopNReportPeriod());

        // 监听动态配置 并更新本对象
        apdexThresholdConfig = new ApdexThresholdConfig(this);
        ApdexMetrics.setDICT(apdexThresholdConfig);
    }

    /**
     * 启动核心模块
     * @throws ModuleStartException
     */
    @Override
    public void start() throws ModuleStartException {

        // 为 grpc 添加处理器  底层应该就是对应 netty的 ChannelHandler   注意这是服务端的处理器 也就是从客户端接收数据 并在这里做统一加工
        // RemoteServiceHandler 的逻辑就是接收其他端发送过来的数据 同样通过worker做处理 如果底层的持久层是共用的 那么意义就不大了 所以有那种 foreverFirst 的模式
        // 比如说每个节点应用 绑定一个 oap 这样就会自行收集数据 然后汇聚到 集群中某个节点后 一起处理数据 (虽然一般就只是保存) 同时更能发挥批处理的功能
        grpcServer.addHandler(new RemoteServiceHandler(getManager()));
        // 该服务节点接收其他client的心跳 并返回响应信息   注意客户端没有发送心跳
        grpcServer.addHandler(new HealthCheckServiceHandler());
        // 开始建立集群内其他节点的连接
        remoteClientManager.start();

        try {
            // 开始扫描 SourceDispatcher 实现类
            receiver.scan();
            // 扫描 model 并创建worker 以及注册worker
            annotationScan.scan();

            // 将脚本中的一些统计类也通过 StreamAnnotationListener 走一遍 创建对应worker  先忽略
            oalEngine.notifyAllListeners();
        } catch (IOException | IllegalAccessException | InstantiationException e) {
            throw new ModuleStartException(e.getMessage(), e);
        }

        // 首先每个节点都会开启一个 rpc入口和一个 http入口 但是只有 Mixed 和 Aggregator 会选择将信息暴露出去 才具备被其他节点发现的功能 如果数据不能被处理那么收集起来该怎么使用呢
        if (CoreModuleConfig.Role.Mixed.name()
                                       .equalsIgnoreCase(
                                           moduleConfig.getRole())
            || CoreModuleConfig.Role.Aggregator.name()
                                               .equalsIgnoreCase(
                                                   moduleConfig.getRole())) {
            RemoteInstance gRPCServerInstance = new RemoteInstance(
                new Address(moduleConfig.getGRPCHost(), moduleConfig.getGRPCPort(), true));
            this.getManager()
                .find(ClusterModule.NAME)
                .provider()
                .getService(ClusterRegister.class)
                .registerRemote(gRPCServerInstance);
        }

        DynamicConfigurationService dynamicConfigurationService = getManager().find(ConfigurationModule.NAME)
                                                                              .provider()
                                                                              .getService(
                                                                                  DynamicConfigurationService.class);
        dynamicConfigurationService.registerConfigChangeWatcher(apdexThresholdConfig);
    }

    /**
     * 启动完成时的后置钩子
     * @throws ModuleStartException
     */
    @Override
    public void notifyAfterCompleted() throws ModuleStartException {
        try {
            grpcServer.start();
            jettyServer.start();
        } catch (ServerException e) {
            throw new ModuleStartException(e.getMessage(), e);
        }

        // 持久化数据相关的 定时器 就是通过它扫描缓存容器 并将数据持久化
        PersistenceTimer.INSTANCE.start(getManager(), moduleConfig);

        // 定期删除一些设置了 TTL 的数据 比如ES
        if (moduleConfig.isEnableDataKeeperExecutor()) {
            DataTTLKeeperTimer.INSTANCE.start(getManager(), moduleConfig);
        }

        // 该对象用于定期更新缓存
        CacheUpdateTimer.INSTANCE.start(getManager());
    }

    @Override
    public String[] requiredModules() {
        return new String[] {
                // 基于遥感模块
            TelemetryModule.NAME,
                // 依赖于配置中心
            ConfigurationModule.NAME
        };
    }
}
