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

/**
 * 模块定义信息  可以通过该对象快捷的获取到某个模块的子信息
 */
public interface ModuleDefineHolder {

    /**
     * 是否包含某个模块
     * @param moduleName
     * @return
     */
    boolean has(String moduleName);

    /**
     * 通过 模块名找到某个模块的提供者
     * @param moduleName
     * @return
     * @throws ModuleNotFoundRuntimeException
     */
    ModuleProviderHolder find(String moduleName) throws ModuleNotFoundRuntimeException;
}
