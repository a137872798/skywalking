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

import java.util.LinkedList;
import java.util.List;
import org.apache.skywalking.apm.agent.core.conf.RemoteDownstreamConfig;
import org.apache.skywalking.apm.agent.core.context.ids.DistributedTraceId;
import org.apache.skywalking.apm.agent.core.context.ids.DistributedTraceIds;
import org.apache.skywalking.apm.agent.core.context.ids.GlobalIdGenerator;
import org.apache.skywalking.apm.agent.core.context.ids.ID;
import org.apache.skywalking.apm.agent.core.context.ids.NewDistributedTraceId;
import org.apache.skywalking.apm.network.language.agent.UpstreamSegment;
import org.apache.skywalking.apm.network.language.agent.v2.SegmentObject;

/**
 * {@link TraceSegment} is a segment or fragment of the distributed trace. See https://github.com/opentracing/specification/blob/master/specification.md#the-opentracing-data-model
 * A {@link TraceSegment} means the segment, which exists in current {@link Thread}. And the distributed trace is formed
 * by multi {@link TraceSegment}s, because the distributed trace crosses multi-processes, multi-threads. <p>
 *     代表一个段对象
 */
public class TraceSegment {
    /**
     * The id of this trace segment. Every segment has its unique-global-id.
     * 每个段有自己的全局id  而一个链路上下文应该包含很多段
     */
    private ID traceSegmentId;

    /**
     * The refs of parent trace segments, except the primary one. For most RPC call, {@link #refs} contains only one
     * element, but if this segment is a start span of batch process, the segment faces multi parents, at this moment,
     * we use this {@code #refs} to link them.
     * <p>
     * This field will not be serialized. Keeping this field is only for quick accessing.
     */
    private List<TraceSegmentRef> refs;

    /**
     * The spans belong to this trace segment. They all have finished. All active spans are hold and controlled by
     * "skywalking-api" module.
     * 每个段内部维护一组span  首先通过 TracingContext 来申请 span 对象 而当某个span 使用完毕后 通过调用finish 会转发到该方法
     */
    private List<AbstractTracingSpan> spans;

    /**
     * The <code>relatedGlobalTraces</code> represent a set of all related trace. Most time it contains only one
     * element, because only one parent {@link TraceSegment} exists, but, in batch scenario, the num becomes greater
     * than 1, also meaning multi-parents {@link TraceSegment}. <p> The difference between
     * <code>relatedGlobalTraces</code> and {@link #refs} is: {@link #refs} targets this {@link TraceSegment}'s direct
     * parent, <p> and <p> <code>relatedGlobalTraces</code> targets this {@link TraceSegment}'s related call chain, a
     * call chain contains multi {@link TraceSegment}s, only using {@link #refs} is not enough for analysis and ui.
     */
    private DistributedTraceIds relatedGlobalTraces;

    /**
     * true 代表之后不需要为该对象生成样本
     */
    private boolean ignore = false;

    /**
     * 该对象在使用过程中 是否创建了过多的span
     */
    private boolean isSizeLimited = false;

    private final long createTime;

    /**
     * Create a default/empty trace segment, with current time as start time, and generate a new segment id.
     */
    public TraceSegment() {
        // 整个链路被分成多级 Segment -> Span
        this.traceSegmentId = GlobalIdGenerator.generate();
        // 存放该 segment 下所有的span 对象
        this.spans = new LinkedList<>();
        // 该对象包含了一组全局id
        this.relatedGlobalTraces = new DistributedTraceIds();
        // 这里又生成了一个全局id 并保存到list 中
        this.relatedGlobalTraces.append(new NewDistributedTraceId());
        this.createTime = System.currentTimeMillis();
    }

    /**
     * Establish the link between this segment and its parents.
     *
     * @param refSegment {@link TraceSegmentRef}
     * 设置一个 ref 对象 ref对象像是 segment的描述信息
     */
    public void ref(TraceSegmentRef refSegment) {
        if (refs == null) {
            refs = new LinkedList<>();
        }
        if (!refs.contains(refSegment)) {
            refs.add(refSegment);
        }
    }

    /**
     * Establish the line between this segment and all relative global trace ids.
     */
    public void relatedGlobalTraces(DistributedTraceId distributedTraceId) {
        relatedGlobalTraces.append(distributedTraceId);
    }

    /**
     * After {@link AbstractSpan} is finished, as be controller by "skywalking-api" module, notify the {@link
     * TraceSegment} to archive it.
     * 当某个 span使用完毕时 调用 .finish 会转发到该方法
     */
    public void archive(AbstractTracingSpan finishedSpan) {
        spans.add(finishedSpan);
    }

    /**
     * Finish this {@link TraceSegment}. <p> return this, for chaining
     * 根据是否创建了过多的 span 来终止该 span 对象
     */
    public TraceSegment finish(boolean isSizeLimited) {
        this.isSizeLimited = isSizeLimited;
        return this;
    }

    public ID getTraceSegmentId() {
        return traceSegmentId;
    }

    public int getServiceId() {
        return RemoteDownstreamConfig.Agent.SERVICE_ID;
    }

    public boolean hasRef() {
        return !(refs == null || refs.size() == 0);
    }

    public List<TraceSegmentRef> getRefs() {
        return refs;
    }

    public List<DistributedTraceId> getRelatedGlobalTraces() {
        return relatedGlobalTraces.getRelatedGlobalTraces();
    }

    public boolean isSingleSpanSegment() {
        return this.spans != null && this.spans.size() == 1;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    /**
     * This is a high CPU cost method, only called when sending to collector or test cases.
     *
     * @return the segment as GRPC service parameter
     */
    public UpstreamSegment transform() {
        UpstreamSegment.Builder upstreamBuilder = UpstreamSegment.newBuilder();
        for (DistributedTraceId distributedTraceId : getRelatedGlobalTraces()) {
            upstreamBuilder = upstreamBuilder.addGlobalTraceIds(distributedTraceId.toUniqueId());
        }
        SegmentObject.Builder traceSegmentBuilder = SegmentObject.newBuilder();
        /*
         * Trace Segment
         */
        traceSegmentBuilder.setTraceSegmentId(this.traceSegmentId.transform());
        // Don't serialize TraceSegmentReference

        // SpanObject
        for (AbstractTracingSpan span : this.spans) {
            traceSegmentBuilder.addSpans(span.transform());
        }
        traceSegmentBuilder.setServiceId(RemoteDownstreamConfig.Agent.SERVICE_ID);
        traceSegmentBuilder.setServiceInstanceId(RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID);
        traceSegmentBuilder.setIsSizeLimited(this.isSizeLimited);

        upstreamBuilder.setSegment(traceSegmentBuilder.build().toByteString());
        return upstreamBuilder.build();
    }

    @Override
    public String toString() {
        return "TraceSegment{" + "traceSegmentId='" + traceSegmentId + '\'' + ", refs=" + refs + ", spans=" + spans + ", relatedGlobalTraces=" + relatedGlobalTraces + '}';
    }

    public int getApplicationInstanceId() {
        return RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID;
    }

    public long createTime() {
        return this.createTime;
    }
}
