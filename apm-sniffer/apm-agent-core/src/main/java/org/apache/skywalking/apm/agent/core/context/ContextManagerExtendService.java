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

package org.apache.skywalking.apm.agent.core.context;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;

/**
 * 该对象负责创建上下文
 */
@DefaultImplementor
public class ContextManagerExtendService implements BootService {
    @Override
    public void prepare() {

    }

    @Override
    public void boot() {

    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {

    }

    /**
     *
     * @param operationName  标记本次操作名称
     * @param forceSampling  是否要生成样本
     * @return
     */
    public AbstractTracerContext createTraceContext(String operationName, boolean forceSampling) {
        AbstractTracerContext context;
        int suffixIdx = operationName.lastIndexOf(".");
        // 包含指定后缀的操作将会被忽略
        if (suffixIdx > -1 && Config.Agent.IGNORE_SUFFIX.contains(operationName.substring(suffixIdx))) {
            context = new IgnoredTracerContext();
        } else {
            // 委托给 样品服务生成上下文
            SamplingService samplingService = ServiceManager.INSTANCE.findService(SamplingService.class);
            // 如果强制要求样本 那么必然生成 链路上下文 如果没有强制要求 那么尝试生成 如果失败的话 返回一个空的上下文
            if (forceSampling || samplingService.trySampling()) {
                context = new TracingContext(operationName);
            } else {
                context = new IgnoredTracerContext();
            }
        }

        return context;
    }
}
