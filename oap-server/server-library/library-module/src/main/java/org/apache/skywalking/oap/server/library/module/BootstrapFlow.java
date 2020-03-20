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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BootstrapFlow {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapFlow.class);

    private Map<String, ModuleDefine> loadedModules;
    private List<ModuleProvider> startupSequence;

    /**
     * 代表本次 oap 加载的一系列模块
     *
     * @param loadedModules
     * @throws CycleDependencyException
     */
    BootstrapFlow(Map<String, ModuleDefine> loadedModules) throws CycleDependencyException {
        this.loadedModules = loadedModules;
        startupSequence = new LinkedList<>();

        makeSequence();
    }

    /**
     * 启动所有的provider
     * @param moduleManager
     * @throws ModuleNotFoundException
     * @throws ServiceNotProvidedException
     * @throws ModuleStartException
     */
    @SuppressWarnings("unchecked")
    void start(ModuleManager moduleManager)
            throws ModuleNotFoundException, ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) {
            String[] requiredModules = provider.requiredModules();
            if (requiredModules != null) {
                for (String module : requiredModules) {
                    if (!moduleManager.has(module)) {
                        throw new ModuleNotFoundException(module + " is required by " + provider.getModuleName() + "." + provider
                                .name() + ", but not found.");
                    }
                }
            }
            logger.info("start the provider {} in {} module.", provider.name(), provider.getModuleName());
            // 确保此时的service 都已经创建成功
            provider.requiredCheck(provider.getModule().services());

            provider.start();
        }
    }

    void notifyAfterCompleted() throws ServiceNotProvidedException, ModuleStartException {
        for (ModuleProvider provider : startupSequence) {
            provider.notifyAfterCompleted();
        }
    }

    /**
     * @throws CycleDependencyException
     */
    private void makeSequence() throws CycleDependencyException {
        List<ModuleProvider> allProviders = new ArrayList<>();
        loadedModules.forEach((moduleName, module) -> allProviders.add(module.provider()));

        do {
            // provider的数量    一个module 对应一个provider
            int numOfToBeSequenced = allProviders.size();
            for (int i = 0; i < allProviders.size(); i++) {
                ModuleProvider provider = allProviders.get(i);
                // 代表某个 provider依赖的其他模块
                String[] requiredModules = provider.requiredModules();
                if (CollectionUtils.isNotEmpty(requiredModules)) {
                    boolean isAllRequiredModuleStarted = true;
                    for (String module : requiredModules) {
                        // find module in all ready existed startupSequence
                        boolean exist = false;
                        // 因为外层还有一个循环 实际上这里会将可以直接使用的 provider 先保存起来(可以这样粗略的理解)
                        for (ModuleProvider moduleProvider : startupSequence) {
                            if (moduleProvider.getModuleName().equals(module)) {
                                exist = true;
                                // 当本次 module 匹配时 退出该循环并获取下一个module
                                break;
                            }
                        }

                        // 只要有一个module 没有生成 provider isAllRequiredModuleStarted 标识为false 代表不满足初始条件
                        if (!exist) {
                            isAllRequiredModuleStarted = false;
                            break;
                        }
                    }

                    // 满足条件的provider 添加到容器中
                    if (isAllRequiredModuleStarted) {
                        startupSequence.add(provider);
                        allProviders.remove(i);
                        i--;
                    }
                } else {
                    // 如果该provider 不依赖其他模块 那么直接添加
                    startupSequence.add(provider);
                    allProviders.remove(i);
                    i--;
                }
            }

            // 如果某轮没有任何的变化 那么代表没有一个provider 是能够正常启用的 (他们存在循环依赖关系)
            if (numOfToBeSequenced == allProviders.size()) {
                StringBuilder unSequencedProviders = new StringBuilder();
                allProviders.forEach(provider -> unSequencedProviders.append(provider.getModuleName())
                        .append("[provider=")
                        .append(provider.getClass().getName())
                        .append("]\n"));
                throw new CycleDependencyException("Exist cycle module dependencies in \n" + unSequencedProviders.substring(0, unSequencedProviders
                        .length() - 1));
            }
        }
        while (allProviders.size() != 0);
    }
}
