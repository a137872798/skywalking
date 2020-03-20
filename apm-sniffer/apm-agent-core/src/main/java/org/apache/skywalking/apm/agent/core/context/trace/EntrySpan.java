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

package org.apache.skywalking.apm.agent.core.context.trace;

import org.apache.skywalking.apm.agent.core.context.TracingContext;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;
import org.apache.skywalking.apm.network.trace.component.Component;

/**
 * The <code>EntrySpan</code> represents a service provider point, such as Tomcat server entrance.
 * <p>
 * It is a start point of {@link TraceSegment}, even in a complex application, there maybe have multi-layer entry point,
 * the <code>EntrySpan</code> only represents the first one.
 * <p>
 * But with the last <code>EntrySpan</code>'s tags and logs, which have more details about a service provider.
 * <p>
 * Such as: Tomcat Embed - Dubbox The <code>EntrySpan</code> represents the Dubbox span.
 * 模型上对应提供者 也就是在提供者端当接收请求时 才会生成 EntrySpan么
 */
public class EntrySpan extends StackBasedTracingSpan {

    /**
     * 当前最大深度
     */
    private int currentMaxDepth;

    /**
     * 初始化该对象时 需要传入上下文对象
     * @param spanId
     * @param parentSpanId
     * @param operationName
     * @param owner
     */
    public EntrySpan(int spanId, int parentSpanId, String operationName, TracingContext owner) {
        super(spanId, parentSpanId, operationName, owner);
        this.currentMaxDepth = 0;
    }

    public EntrySpan(int spanId, int parentSpanId, int operationId, TracingContext owner) {
        super(spanId, parentSpanId, operationId, owner);
        this.currentMaxDepth = 0;
    }

    /**
     * Set the {@link #startTime}, when the first start, which means the first service provided.
     * 开始调用时  该方法应该会调用多次
     */
    @Override
    public EntrySpan start() {
        // 只有第一次设置才会 设置start时间
        if ((currentMaxDepth = ++stackDepth) == 1) {
            super.start();
        }
        // 当调用start 时做一些清理工作
        clearWhenRestart();
        return this;
    }

    // 深度匹配的时候才能正常操作 看来某些时候  stackDepth 与 currentMaxDepth 不会匹配
    @Override
    public EntrySpan tag(String key, String value) {
        if (stackDepth == currentMaxDepth) {
            super.tag(key, value);
        }
        return this;
    }

    @Override
    public AbstractTracingSpan setLayer(SpanLayer layer) {
        if (stackDepth == currentMaxDepth) {
            return super.setLayer(layer);
        } else {
            return this;
        }
    }

    @Override
    public AbstractTracingSpan setComponent(Component component) {
        if (stackDepth == currentMaxDepth) {
            return super.setComponent(component);
        } else {
            return this;
        }
    }

    @Override
    public AbstractTracingSpan setComponent(String componentName) {
        if (stackDepth == currentMaxDepth) {
            return super.setComponent(componentName);
        } else {
            return this;
        }
    }

    @Override
    public AbstractTracingSpan setOperationName(String operationName) {
        // TODO 如果是异步模式 也允许设置 operateName 是什么意思???
        if (stackDepth == currentMaxDepth || isInAsyncMode) {
            return super.setOperationName(operationName);
        } else {
            return this;
        }
    }

    @Override
    public AbstractTracingSpan setOperationId(int operationId) {
        if (stackDepth == currentMaxDepth) {
            return super.setOperationId(operationId);
        } else {
            return this;
        }
    }

    @Override
    public EntrySpan log(Throwable t) {
        super.log(t);
        return this;
    }

    @Override
    public boolean isEntry() {
        return true;
    }

    @Override
    public boolean isExit() {
        return false;
    }

    private void clearWhenRestart() {
        this.componentId = DictionaryUtil.nullValue();
        this.componentName = null;
        this.layer = null;
        this.logs = null;
        this.tags = null;
    }
}
