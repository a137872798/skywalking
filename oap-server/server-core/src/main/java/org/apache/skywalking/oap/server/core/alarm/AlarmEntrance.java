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

package org.apache.skywalking.oap.server.core.alarm;

import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;

public class AlarmEntrance {

    /**
     * 该对象可以通过模块名 找到一个 ModuleProvider 对象  provider可以获取到一个 ModuleServiceHolder
     * moduleServiceHolder 内部又包含了一组服务 可以通过服务类型 获取对应实例
     */
    private ModuleDefineHolder moduleDefineHolder;
    /**
     * 该对象用于通知统计数据
     */
    private MetricsNotify metricsNotify;

    public AlarmEntrance(ModuleDefineHolder moduleDefineHolder) {
        this.moduleDefineHolder = moduleDefineHolder;
    }

    /**
     * 将测量数据通知到某个地方
     * @param metrics
     */
    public void forward(Metrics metrics) {
        // 如果当前没有 alarm模块 直接返回
        if (!moduleDefineHolder.has(AlarmModule.NAME)) {
            return;
        }

        init();

        metricsNotify.notify(metrics);
    }

    private void init() {
        if (metricsNotify == null) {
            // 如果notify 对象还没有创建 那么从moduleDefine中找到服务实例
            metricsNotify = moduleDefineHolder.find(AlarmModule.NAME).provider().getService(MetricsNotify.class);
        }
    }
}
