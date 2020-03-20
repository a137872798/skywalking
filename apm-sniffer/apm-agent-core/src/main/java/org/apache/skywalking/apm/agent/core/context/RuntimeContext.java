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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.skywalking.apm.agent.core.conf.RuntimeContextConfiguration;

/**
 * RuntimeContext is alive during the tracing context. It will not be serialized to the collector, and always stays in
 * the same context only.
 * <p>
 * In most cases, it means it only stays in a single thread for context propagation.
 * 运行时上下文信息
 */
public class RuntimeContext {
    private final ThreadLocal<RuntimeContext> contextThreadLocal;

    /**
     * 运行时上下文就是绑定在本线程的 一个 map对象
     */
    private Map<Object, Object> context = new ConcurrentHashMap<>(0);

    public RuntimeContext(ThreadLocal<RuntimeContext> contextThreadLocal) {
        this.contextThreadLocal = contextThreadLocal;
    }

    public void put(Object key, Object value) {
        context.put(key, value);
    }

    public Object get(Object key) {
        return context.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Class<T> type) {
        return (T) context.get(key);
    }

    public void remove(Object key) {
        context.remove(key);

        // 本对象内部已经没有元素时 就将 ThreadLocal 进行回收 避免内存泄露
        if (context.isEmpty()) {
            contextThreadLocal.remove();
        }
    }

    /**
     * 返回RuntimeContext的快照 （主要就是看map内的数据）
     * @return
     */
    public RuntimeContextSnapshot capture() {
        Map<Object, Object> runtimeContextMap = new HashMap<>();
        // 3个特殊的key SW_REQUEST SW_RESPONSE SW_WEBFLUX_REQUEST_KEY
        for (String key : RuntimeContextConfiguration.NEED_PROPAGATE_CONTEXT_KEY) {
            // 这里从 ConcurrentHashMap 读取的数据 与写入HashMap 没有使用锁 所以读取到的数据可能已经过时 也就是快照数据(同时该场景本身就没有必要使用强一致性)
            Object value = this.get(key);
            if (value != null) {
                runtimeContextMap.put(key, value);
            }
        }

        return new RuntimeContextSnapshot(runtimeContextMap);
    }

    /**
     * 从某个快照中 将数据设置到 RuntimeContext 上
     * @param snapshot
     */
    public void accept(RuntimeContextSnapshot snapshot) {
        Iterator<Map.Entry<Object, Object>> iterator = snapshot.iterator();
        while (iterator.hasNext()) {
            Map.Entry<Object, Object> runtimeContextItem = iterator.next();
            ContextManager.getRuntimeContext().put(runtimeContextItem.getKey(), runtimeContextItem.getValue());
        }
    }
}
