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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.TracingContext;
import org.apache.skywalking.apm.agent.core.context.tag.AbstractTag;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.util.KeyValuePair;
import org.apache.skywalking.apm.agent.core.context.util.TagValuePair;
import org.apache.skywalking.apm.agent.core.context.util.ThrowableTransformer;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;
import org.apache.skywalking.apm.network.language.agent.SpanType;
import org.apache.skywalking.apm.network.language.agent.v2.SpanObjectV2;
import org.apache.skywalking.apm.network.trace.component.Component;

/**
 * The <code>AbstractTracingSpan</code> represents a group of {@link AbstractSpan} implementations, which belongs a real
 * distributed trace.
 * span 骨架类
 */
public abstract class AbstractTracingSpan implements AbstractSpan {

    /**
     * id
     */
    protected int spanId;

    protected int parentSpanId;
    /**
     * 内部包含了一组标签
     */
    protected List<TagValuePair> tags;

    // operationName 和 operationId 好像只能设置一个

    /**
     * 标记本次的操作
     */
    protected String operationName;
    /**
     * 操作对应的id
     */
    protected int operationId;
    /**
     * 标记跨进程使用的方式
     */
    protected SpanLayer layer;
    /**
     * The span has been tagged in async mode, required async stop to finish.
     * 是否是异步执行  比如 dubbo的异步调用吗
     */
    protected volatile boolean isInAsyncMode = false;
    /**
     * The flag represents whether the span has been async stopped
     * 异步执行是否结束
     */
    private volatile boolean isAsyncStopped = false;

    /**
     * The context to which the span belongs
     * 一个完整的调用 对应一个context 下面可能会有多个span
     */
    protected final TracingContext owner;

    /**
     * The start time of this Span.
     * 本span的执行时间
     */
    protected long startTime;
    /**
     * The end time of this Span.
     * 本span的结束时间
     */
    protected long endTime;
    /**
     * Error has occurred in the scope of span.
     */
    protected boolean errorOccurred = false;

    /**
     * 跨进程使用的组件
     */
    protected int componentId = 0;

    protected String componentName;

    /**
     * Log is a concept from OpenTracing spec. https://github.com/opentracing/specification/blob/master/specification.md#log-structured-data
     * 代表日志信息
     */
    protected List<LogDataEntity> logs;

    /**
     * The refs of parent trace segments, except the primary one. For most RPC call, {@link #refs} contains only one
     * element, but if this segment is a start span of batch process, the segment faces multi parents, at this moment,
     * we use this {@link #refs} to link them.
     * 该span 依赖的segment (由这个segment 发起的请求间接创建了这个span对象)
     */
    protected List<TraceSegmentRef> refs;

    protected AbstractTracingSpan(int spanId, int parentSpanId, String operationName, TracingContext owner) {
        this.operationName = operationName;
        this.operationId = DictionaryUtil.nullValue();
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.owner = owner;
    }

    protected AbstractTracingSpan(int spanId, int parentSpanId, int operationId, TracingContext owner) {
        this.operationName = null;
        this.operationId = operationId;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.owner = owner;
    }

    /**
     * Set a key:value tag on the Span.
     * <p>
     * {@inheritDoc}
     *
     * @return this Span instance, for chaining
     */
    @Override
    public AbstractTracingSpan tag(String key, String value) {
        return tag(Tags.ofKey(key), value);
    }

    /**
     * 设置一个标签到 span内
     * @param tag
     * @param value
     * @return
     */
    @Override
    public AbstractTracingSpan tag(AbstractTag<?> tag, String value) {
        if (tags == null) {
            tags = new ArrayList<>(8);
        }

        // 如果标签是可重写的 那么就可能是覆盖原来的属性
        if (tag.isCanOverwrite()) {
            for (TagValuePair pair : tags) {
                if (pair.sameWith(tag)) {
                    pair.setValue(value);
                    return this;
                }
            }
        }

        // 否则添加一个新标签
        tags.add(new TagValuePair(tag, value));
        return this;
    }

    /**
     * Finish the active Span. When it is finished, it will be archived by the given {@link TraceSegment}, which owners
     * it.
     *
     * @param owner of the Span.
     *              代表本次跨进程调用结束了
     */
    public boolean finish(TraceSegment owner) {
        this.endTime = System.currentTimeMillis();
        // 执行完毕的某个span 会添加到 segment 中
        owner.archive(this);
        return true;
    }

    /**
     * 代表开始跨进程
     * @return
     */
    @Override
    public AbstractTracingSpan start() {
        this.startTime = System.currentTimeMillis();
        return this;
    }

    /**
     * Record an exception event of the current walltime timestamp.
     *
     * @param t any subclass of {@link Throwable}, which occurs in this span.
     * @return the Span, for chaining
     */
    @Override
    public AbstractTracingSpan log(Throwable t) {
        if (logs == null) {
            logs = new LinkedList<>();
        }
        logs.add(new LogDataEntity.Builder().add(new KeyValuePair("event", "error"))
                                            .add(new KeyValuePair("error.kind", t.getClass().getName()))
                                            .add(new KeyValuePair("message", t.getMessage()))
                                            .add(new KeyValuePair("stack", ThrowableTransformer.INSTANCE.convert2String(t, 4000)))
                                            .build(System.currentTimeMillis()));
        return this;
    }

    /**
     * Record a common log with multi fields, for supporting opentracing-java
     *
     * @return the Span, for chaining
     * 使用map内的数据来初始化
     */
    @Override
    public AbstractTracingSpan log(long timestampMicroseconds, Map<String, ?> fields) {
        if (logs == null) {
            logs = new LinkedList<>();
        }
        LogDataEntity.Builder builder = new LogDataEntity.Builder();
        for (Map.Entry<String, ?> entry : fields.entrySet()) {
            builder.add(new KeyValuePair(entry.getKey(), entry.getValue().toString()));
        }
        logs.add(builder.build(timestampMicroseconds));
        return this;
    }

    /**
     * In the scope of this span tracing context, error occurred, in auto-instrumentation mechanism, almost means throw
     * an exception.
     *
     * @return span instance, for chaining.
     * 标记出现了异常
     */
    @Override
    public AbstractTracingSpan errorOccurred() {
        this.errorOccurred = true;
        return this;
    }

    /**
     * Set the operation name, just because these is not compress dictionary value for this name. Use the entire string
     * temporarily, the agent will compress this name in async mode.
     *
     * @return span instance, for chaining.
     * 设置本次跨进程的操作
     */
    @Override
    public AbstractTracingSpan setOperationName(String operationName) {
        this.operationName = operationName;
        this.operationId = DictionaryUtil.nullValue();

        // recheck profiling status
        // 这里是在检查 span的信息
        owner.profilingRecheck(this, operationName);
        return this;
    }

    /**
     * Set the operation id, which compress by the name.
     *
     * @return span instance, for chaining.
     */
    @Override
    public AbstractTracingSpan setOperationId(int operationId) {
        this.operationId = operationId;
        this.operationName = null;
        return this;
    }

    @Override
    public int getSpanId() {
        return spanId;
    }

    @Override
    public int getOperationId() {
        return operationId;
    }

    @Override
    public String getOperationName() {
        return operationName;
    }

    @Override
    public AbstractTracingSpan setLayer(SpanLayer layer) {
        this.layer = layer;
        return this;
    }

    /**
     * Set the component of this span, with internal supported. Highly recommend to use this way.
     *
     * @return span instance, for chaining.
     */
    @Override
    public AbstractTracingSpan setComponent(Component component) {
        this.componentId = component.getId();
        return this;
    }

    /**
     * Set the component name. By using this, cost more memory and network.
     *
     * @return span instance, for chaining.
     */
    @Override
    public AbstractTracingSpan setComponent(String componentName) {
        this.componentName = componentName;
        return this;
    }

    @Override
    public AbstractSpan start(long startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * 将当前信息转换成 符合 protobuf格式的数据
     * @return
     */
    public SpanObjectV2.Builder transform() {
        SpanObjectV2.Builder spanBuilder = SpanObjectV2.newBuilder();

        spanBuilder.setSpanId(this.spanId);
        spanBuilder.setParentSpanId(parentSpanId);
        spanBuilder.setStartTime(startTime);
        spanBuilder.setEndTime(endTime);
        if (operationId != DictionaryUtil.nullValue()) {
            spanBuilder.setOperationNameId(operationId);
        } else {
            spanBuilder.setOperationName(operationName);
        }
        if (isEntry()) {
            spanBuilder.setSpanType(SpanType.Entry);
        } else if (isExit()) {
            spanBuilder.setSpanType(SpanType.Exit);
        } else {
            spanBuilder.setSpanType(SpanType.Local);
        }
        if (this.layer != null) {
            spanBuilder.setSpanLayerValue(this.layer.getCode());
        }
        if (componentId != DictionaryUtil.nullValue()) {
            spanBuilder.setComponentId(componentId);
        } else {
            if (componentName != null) {
                spanBuilder.setComponent(componentName);
            }
        }
        spanBuilder.setIsError(errorOccurred);
        if (this.tags != null) {
            for (TagValuePair tag : this.tags) {
                spanBuilder.addTags(tag.transform());
            }
        }
        if (this.logs != null) {
            for (LogDataEntity log : this.logs) {
                spanBuilder.addLogs(log.transform());
            }
        }
        if (this.refs != null) {
            for (TraceSegmentRef ref : this.refs) {
                spanBuilder.addRefs(ref.transform());
            }
        }

        return spanBuilder;
    }

    /**
     * 代表本次创建的span 是由某个 segment 发起的
     * @param ref segment ref
     */
    @Override
    public void ref(TraceSegmentRef ref) {
        if (refs == null) {
            refs = new LinkedList<>();
        }
        if (!refs.contains(ref)) {
            refs.add(ref);
        }
    }

    /**
     * 准备以异步方式调用
     * @return
     */
    @Override
    public AbstractSpan prepareForAsync() {
        if (isInAsyncMode) {
            throw new RuntimeException("Prepare for async repeatedly. Span is already in async mode.");
        }
        // 做一些准备工作
        ContextManager.awaitFinishAsync(this);
        isInAsyncMode = true;
        return this;
    }

    /**
     * 代表某个异步调用结束了
     * @return
     */
    @Override
    public AbstractSpan asyncFinish() {
        // 必须确保当前已经被设置成异步模式了
        if (!isInAsyncMode) {
            throw new RuntimeException("Span is not in async mode, please use '#prepareForAsync' to active.");
        }
        if (isAsyncStopped) {
            throw new RuntimeException("Can not do async finish for the span repeately.");
        }
        this.endTime = System.currentTimeMillis();
        // 通知异步调用停止
        owner.asyncStop(this);
        // 标记成异步调用已结束
        isAsyncStopped = true;
        return this;
    }
}
