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

import java.lang.reflect.Field;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A module definition.
 * 包含了 模块的提供者 以及模块的名称
 */
public abstract class ModuleDefine implements ModuleProviderHolder {

    private static final Logger logger = LoggerFactory.getLogger(ModuleDefine.class);

    /**
     * 实际上module 代表提供一组Service 的一个集合体  provider对象也只是根据需求返回不同的service
     */
    private ModuleProvider loadedProvider = null;

    private final String name;

    public ModuleDefine(String name) {
        this.name = name;
    }

    /**
     * @return the module name
     */
    public final String name() {
        return name;
    }

    /**
     * @return the {@link Service} provided by this module.
     */
    public abstract Class[] services();

    /**
     * Run the prepare stage for the module, including finding all potential providers, and asking them to prepare.
     *
     * @param moduleManager of this module  该模块交由该manager 进行管理
     * @param configuration of this module  对应的配置对象
     * @throws ProviderNotFoundException when even don't find a single one providers.
     */
    void prepare(ModuleManager moduleManager, ApplicationConfiguration.ModuleConfiguration configuration,
        ServiceLoader<ModuleProvider> moduleProviderLoader  // 一个模块对应很多个提供者 也就对应yml的二级标签
    ) throws ProviderNotFoundException, ServiceNotProvidedException, ModuleConfigException, ModuleStartException {
        for (ModuleProvider provider : moduleProviderLoader) {
            // 没有找到相关的配置就跳过
            if (!configuration.has(provider.name())) {
                continue;
            }

            // 确保当前读取到的 provider 属于这个module
            if (provider.module().equals(getClass())) {
                if (loadedProvider == null) {
                    loadedProvider = provider;
                    loadedProvider.setManager(moduleManager);
                    loadedProvider.setModuleDefine(this);
                } else {
                    // 虽然能在 yml 中为多个provider配置 但是一个模块下只能有一个provider
                    throw new DuplicateProviderException(this.name() + " module has one " + loadedProvider.name() + "[" + loadedProvider
                        .getClass()
                        .getName() + "] provider already, " + provider.name() + "[" + provider.getClass()
                                                                                              .getName() + "] is defined as 2nd provider.");
                }
            }

        }

        if (loadedProvider == null) {
            throw new ProviderNotFoundException(this.name() + " module no provider exists.");
        }

        logger.info("Prepare the {} provider in {} module.", loadedProvider.name(), this.name());
        try {
            // 将 ApplicationConfiguration 的信息 转移到 loaderProvider的Config中
            copyProperties(loadedProvider.createConfigBeanIfAbsent(), configuration.getProviderConfiguration(loadedProvider
                .name()), this.name(), loadedProvider.name());
        } catch (IllegalAccessException e) {
            throw new ModuleConfigException(this.name() + " module config transport to config bean failure.", e);
        }
        // 触发 provider的prepare  实际上是一个钩子函数 应该就是使用config 内部的属性去设置什么
        loadedProvider.prepare();
    }

    /**
     *
     * @param dest  该类内部的属性是自定义的
     * @param src   本次yml读取出的相关属性
     * @param moduleName
     * @param providerName
     * @throws IllegalAccessException
     */
    private void copyProperties(ModuleConfig dest, Properties src, String moduleName,
        String providerName) throws IllegalAccessException {
        if (dest == null) {
            return;
        }
        // 通过反射填充属性 (因为类是动态的所以只能通过反射的方式添加属性)
        Enumeration<?> propertyNames = src.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String propertyName = (String) propertyNames.nextElement();
            Class<? extends ModuleConfig> destClass = dest.getClass();
            try {
                Field field = getDeclaredField(destClass, propertyName);
                field.setAccessible(true);
                field.set(dest, src.get(propertyName));
            } catch (NoSuchFieldException e) {
                logger.warn(propertyName + " setting is not supported in " + providerName + " provider of " + moduleName + " module");
            }
        }
    }

    private Field getDeclaredField(Class<?> destClass, String fieldName) throws NoSuchFieldException {
        if (destClass != null) {
            Field[] fields = destClass.getDeclaredFields();
            for (Field field : fields) {
                if (field.getName().equals(fieldName)) {
                    return field;
                }
            }
            return getDeclaredField(destClass.getSuperclass(), fieldName);
        }

        throw new NoSuchFieldException();
    }

    @Override
    public final ModuleProvider provider() throws DuplicateProviderException, ProviderNotFoundException {
        if (loadedProvider == null) {
            throw new ProviderNotFoundException("There is no module provider in " + this.name() + " module!");
        }

        return loadedProvider;
    }
}
