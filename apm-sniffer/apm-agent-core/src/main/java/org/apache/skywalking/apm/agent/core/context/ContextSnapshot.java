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

import java.util.List;
import org.apache.skywalking.apm.agent.core.context.ids.DistributedTraceId;
import org.apache.skywalking.apm.agent.core.context.ids.ID;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;

/**
 * The <code>ContextSnapshot</code> is a snapshot for current context. The snapshot carries the info for building
 * reference between two segments in two thread, but have a causal relationship.
 * 当前上下文的快照
 */
public class ContextSnapshot {
    /**
     * trace segment id of the parent trace segment.
     * 一个完整的链路应该是 由多个段组成的 返回的快照对应某个段的信息
     */
    private ID traceSegmentId;

    /**
     * span id of the parent span, in parent trace segment.
     */
    private int spanId = -1;

    private String entryOperationName;

    private String parentOperationName;

    /**
     * {@link DistributedTraceId}
     */
    private DistributedTraceId primaryDistributedTraceId;

    /**
     * 应用实例的入口
     */
    private int entryApplicationInstanceId = DictionaryUtil.nullValue();

    /**
     * @param traceSegmentId 推测该id 父级段id
     * @param spanId  父span
     * @param distributedTraceIds 本次整个链路中涉及到的所有id    第一个segment就是开启整个链路的起点
     */
    ContextSnapshot(ID traceSegmentId, int spanId, List<DistributedTraceId> distributedTraceIds) {
        this.traceSegmentId = traceSegmentId;
        this.spanId = spanId;
        if (distributedTraceIds != null) {
            this.primaryDistributedTraceId = distributedTraceIds.get(0);
        }
    }

    public void setEntryOperationName(String entryOperationName) {
        this.entryOperationName = "#" + entryOperationName;
    }

    public void setEntryOperationId(int entryOperationId) {
        this.entryOperationName = entryOperationId + "";
    }

    public void setParentOperationName(String parentOperationName) {
        this.parentOperationName = "#" + parentOperationName;
    }

    public void setParentOperationId(int parentOperationId) {
        this.parentOperationName = parentOperationId + "";
    }

    /**
     * 获取整个链路中 涉及到的所有链路的首个id
     * @return
     */
    public DistributedTraceId getDistributedTraceId() {
        return primaryDistributedTraceId;
    }

    public ID getTraceSegmentId() {
        return traceSegmentId;
    }

    public int getSpanId() {
        return spanId;
    }

    public String getParentOperationName() {
        return parentOperationName;
    }

    public boolean isValid() {
        return traceSegmentId != null && spanId > -1 && entryApplicationInstanceId != DictionaryUtil.nullValue() && primaryDistributedTraceId != null;
    }

    public String getEntryOperationName() {
        return entryOperationName;
    }

    public void setEntryApplicationInstanceId(int entryApplicationInstanceId) {
        this.entryApplicationInstanceId = entryApplicationInstanceId;
    }

    public int getEntryApplicationInstanceId() {
        return entryApplicationInstanceId;
    }

    public boolean isFromCurrent() {
        return traceSegmentId.equals(ContextManager.capture().getTraceSegmentId());
    }
}
