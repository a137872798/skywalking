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

import java.util.HashMap;
import java.util.Properties;

/**
 * Modulization configurations. The {@link ModuleManager} is going to start, lookup, start modules based on this.
 * 应用相关的配置  相当于是一个总的配置访问入口
 */
public class ApplicationConfiguration {

    /**
     * 内部根据模块进行细化
     */
    private HashMap<String, ModuleConfiguration> modules = new HashMap<>();

    public String[] moduleList() {
        return modules.keySet().toArray(new String[0]);
    }

    public ModuleConfiguration addModule(String moduleName) {
        ModuleConfiguration newModule = new ModuleConfiguration();
        modules.put(moduleName, newModule);
        return newModule;
    }

    public boolean has(String moduleName) {
        return modules.containsKey(moduleName);
    }

    public ModuleConfiguration getModuleConfiguration(String name) {
        return modules.get(name);
    }

    /**
     * The configurations about a certain module.
     * 模块相关的配置
     */
    public class ModuleConfiguration {

        /**
         * ProviderConfiguration 是 prop 的包装对象
         * module 下还有子级配置
         */
        private HashMap<String, ProviderConfiguration> providers = new HashMap<>();

        private ModuleConfiguration() {
        }

        /**
         * 通过子级配置名 找到对应的prop
         * @param name
         * @return
         */
        public Properties getProviderConfiguration(String name) {
            return providers.get(name).getProperties();
        }

        public boolean has(String name) {
            return providers.containsKey(name);
        }

        public ModuleConfiguration addProviderConfiguration(String name, Properties properties) {
            ProviderConfiguration newProvider = new ProviderConfiguration(properties);
            providers.put(name, newProvider);
            return this;
        }
    }

    /**
     * The configuration about a certain provider of a module.
     */
    public class ProviderConfiguration {
        private Properties properties;

        ProviderConfiguration(Properties properties) {
            this.properties = properties;
        }

        private Properties getProperties() {
            return properties;
        }
    }
}
