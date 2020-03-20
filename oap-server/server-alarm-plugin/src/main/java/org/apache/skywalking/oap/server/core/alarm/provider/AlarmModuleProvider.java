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

import java.io.FileNotFoundException;
import java.io.Reader;
import org.apache.skywalking.oap.server.configuration.api.ConfigurationModule;
import org.apache.skywalking.oap.server.configuration.api.DynamicConfigurationService;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.alarm.AlarmModule;
import org.apache.skywalking.oap.server.core.alarm.AlarmStandardPersistence;
import org.apache.skywalking.oap.server.core.alarm.MetricsNotify;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;
import org.apache.skywalking.oap.server.library.module.ModuleDefine;
import org.apache.skywalking.oap.server.library.module.ModuleProvider;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.module.ServiceNotProvidedException;
import org.apache.skywalking.oap.server.library.util.ResourceUtils;

/**
 * 该对象对应 警报module 服务提供类
 */
public class AlarmModuleProvider extends ModuleProvider {

    private NotifyHandler notifyHandler;
    private AlarmRulesWatcher alarmRulesWatcher;

    @Override
    public String name() {
        return "default";
    }

    @Override
    public Class<? extends ModuleDefine> module() {
        return AlarmModule.class;
    }

    @Override
    public ModuleConfig createConfigBeanIfAbsent() {
        return new AlarmSettings();
    }

    /**
     * 当从 yml中抽取相关信息并设置到 createConfigBeanIfAbsent 返回的配置类后 准备生成 Service
     * @throws ServiceNotProvidedException
     * @throws ModuleStartException
     */
    @Override
    public void prepare() throws ServiceNotProvidedException, ModuleStartException {
        Reader applicationReader;
        try {
            // 这里从另外一个文件单独读取配置信息
            applicationReader = ResourceUtils.read("alarm-settings.yml");
        } catch (FileNotFoundException e) {
            throw new ModuleStartException("can't load alarm-settings.yml", e);
        }
        RulesReader reader = new RulesReader(applicationReader);
        Rules rules = reader.readRules();

        // 使用指定配置生成哨兵  (就是将rule信息填充到内部实体)
        alarmRulesWatcher = new AlarmRulesWatcher(rules, this);

        // 生成 Service 对象
        notifyHandler = new NotifyHandler(alarmRulesWatcher);
        // 通过 core 对象开启定时任务 之后警报信息会触发回调对象
        notifyHandler.init(new AlarmStandardPersistence());
        this.registerServiceImplementation(MetricsNotify.class, notifyHandler);
    }

    /**
     * 启动时 寻找动态配置 并注册watcher
     * @throws ServiceNotProvidedException
     * @throws ModuleStartException
     */
    @Override
    public void start() throws ServiceNotProvidedException, ModuleStartException {
        DynamicConfigurationService dynamicConfigurationService = getManager().find(ConfigurationModule.NAME)
                                                                              .provider()
                                                                              .getService(DynamicConfigurationService.class);
        dynamicConfigurationService.registerConfigChangeWatcher(alarmRulesWatcher);
    }

    /**
     * 当所有provider 都启动完成时触发的钩子
     * @throws ServiceNotProvidedException
     * @throws ModuleStartException
     */
    @Override
    public void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {
        notifyHandler.initCache(getManager());
    }

    @Override
    public String[] requiredModules() {
        return new String[] {
            CoreModule.NAME,
            ConfigurationModule.NAME
        };
    }
}
