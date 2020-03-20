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

package org.apache.skywalking.oap.server.library.module;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The <code>ModuleManager</code> takes charge of all {@link ModuleDefine}s in collector.
 * 模块管理器 外部访问模块信息的入口
 */
public class ModuleManager implements ModuleDefineHolder {

    private boolean isInPrepareStage = true;
    private final Map<String, ModuleDefine> loadedModules = new HashMap<>();

    /**
     * Init the given modules
     */
    public void init(ApplicationConfiguration applicationConfiguration)
            throws ModuleNotFoundException, ProviderNotFoundException, ServiceNotProvidedException, CycleDependencyException, ModuleConfigException, ModuleStartException {
        String[] moduleNames = applicationConfiguration.moduleList();
        // 模块名不是随便写的  对应的实现类一开始就存在于  skywalking 中 也可以通过实现接口配合SPI 满足定制要求
        ServiceLoader<ModuleDefine> moduleServiceLoader = ServiceLoader.load(ModuleDefine.class);
        ServiceLoader<ModuleProvider> moduleProviderLoader = ServiceLoader.load(ModuleProvider.class);

        LinkedList<String> moduleList = new LinkedList<>(Arrays.asList(moduleNames));
        // 在迭代的时候 就会实例化各个类
        for (ModuleDefine module : moduleServiceLoader) {
            for (String moduleName : moduleNames) {
                // 找到匹配的配置
                if (moduleName.equals(module.name())) {
                    ModuleDefine newInstance;
                    try {
                        newInstance = module.getClass().newInstance();
                    } catch (InstantiationException | IllegalAccessException e) {
                        throw new ModuleNotFoundException(e);
                    }
                    // 获取对应配置 对module 进行装配
                    newInstance.prepare(this, applicationConfiguration.getModuleConfiguration(moduleName), moduleProviderLoader);
                    loadedModules.put(moduleName, newInstance);
                    moduleList.remove(moduleName);
                }
            }
        }
        // Finish prepare stage
        isInPrepareStage = false;

        // 代表有某个模块没有找到对应的Define类
        if (moduleList.size() > 0) {
            throw new ModuleNotFoundException(moduleList.toString() + " missing.");
        }

        // 这里会判断确保provider都能正常初始化 因为可能存在循环依赖的问题
        BootstrapFlow bootstrapFlow = new BootstrapFlow(loadedModules);

        // 启动下面所有的provider
        bootstrapFlow.start(this);
        bootstrapFlow.notifyAfterCompleted();
    }

    @Override
    public boolean has(String moduleName) {
        return loadedModules.get(moduleName) != null;
    }

    /**
     * 通过模块名找到对应的 provider
     * @param moduleName
     * @return
     * @throws ModuleNotFoundRuntimeException
     */
    @Override
    public ModuleProviderHolder find(String moduleName) throws ModuleNotFoundRuntimeException {
        assertPreparedStage();
        ModuleDefine module = loadedModules.get(moduleName);
        if (module != null)
            return module;
        throw new ModuleNotFoundRuntimeException(moduleName + " missing.");
    }

    private void assertPreparedStage() {
        if (isInPrepareStage) {
            throw new AssertionError("Still in preparing stage.");
        }
    }
}
