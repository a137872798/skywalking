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

import java.io.Serializable;
import java.util.List;
import org.apache.skywalking.apm.agent.core.base64.Base64;
import org.apache.skywalking.apm.agent.core.context.ids.DistributedTraceId;
import org.apache.skywalking.apm.agent.core.context.ids.ID;
import org.apache.skywalking.apm.agent.core.context.ids.PropagatedTraceId;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;
import org.apache.skywalking.apm.util.StringUtil;

/**
 * {@link ContextCarrier} is a data carrier of {@link TracingContext}. It holds the snapshot (current state) of {@link
 * TracingContext}.
 * <p>
 *     该对象存储了当前上下文的状态
 */
public class ContextCarrier implements Serializable {

    /**
     * 应该是最上层的 segment
     */
    private ID traceSegmentId;

    /**
     * id of parent span. It is unique in parent trace segment.
     * 最上层的span
     */
    private int spanId = -1;

    /**
     * id of parent application instance, it's the id assigned by collector.
     * 最上层的 服务实例id
     */
    private int parentServiceInstanceId = DictionaryUtil.nullValue();

    /**
     * id of first application instance in this distributed trace, it's the id assigned by collector.
     * 分布式链路的第一个服务实例
     */
    private int entryServiceInstanceId = DictionaryUtil.nullValue();

    /**
     * peer(ipv4s/ipv6/hostname + port) of the server, from client side.
     * 代表服务器端的地址信息 当本机作为消费者时 provider 就作为server
     */
    private String peerHost;

    /**
     * Operation/Service name of the first one in this distributed trace. This name may be compressed to an integer.
     * 入口的 端点名
     */
    private String entryEndpointName;

    /**
     * Operation/Service name of the parent one in this distributed trace. This name may be compressed to an integer.
     */
    private String parentEndpointName;

    /**
     * {@link DistributedTraceId}, also known as TraceId
     * 主要的链路id???
     */
    private DistributedTraceId primaryDistributedTraceId;

    public CarrierItem items() {
        SW6CarrierItem sw6CarrierItem = new SW6CarrierItem(this, null);
        return new CarrierItemHead(sw6CarrierItem);
    }

    /**
     * Serialize this {@link ContextCarrier} to a {@link String}, with '|' split.
     *
     * @return the serialization string.
     * 格式化输出信息
     */
    String serialize(HeaderVersion version) {
        if (this.isValid(version)) {
            return StringUtil.join(
                '-',
                "1",
                Base64.encode(this.getPrimaryDistributedTraceId().encode()),
                Base64.encode(this.getTraceSegmentId().encode()),
                this.getSpanId() + "",
                this.getParentServiceInstanceId() + "",
                this.getEntryServiceInstanceId() + "",
                Base64.encode(this.getPeerHost()),
                Base64.encode(this.getEntryEndpointName()),
                Base64.encode(this.getParentEndpointName())
            );
        }
        return "";
    }

    /**
     * Initialize fields with the given text.
     *
     * @param text carries {@link #traceSegmentId} and {@link #spanId}, with '|' split.
     *             使用一个字符串来填充carrier内部的属性  代表着 carrier的初始化  context的很多方法都要先校验是否完成了初始化
     */
    ContextCarrier deserialize(String text, HeaderVersion version) {
        if (text == null) {
            return this;
        }
        // if this carrier is initialized by v2, don't do deserialize again for performance.
        if (this.isValid(HeaderVersion.v2)) {
            return this;
        }
        if (HeaderVersion.v2 == version) {
            String[] parts = text.split("-", 9);
            if (parts.length == 9) {
                try {
                    // parts[0] is sample flag, always trace if header exists.
                    this.primaryDistributedTraceId = new PropagatedTraceId(Base64.decode2UTFString(parts[1]));
                    this.traceSegmentId = new ID(Base64.decode2UTFString(parts[2]));
                    this.spanId = Integer.parseInt(parts[3]);
                    this.parentServiceInstanceId = Integer.parseInt(parts[4]);
                    this.entryServiceInstanceId = Integer.parseInt(parts[5]);
                    this.peerHost = Base64.decode2UTFString(parts[6]);
                    this.entryEndpointName = Base64.decode2UTFString(parts[7]);
                    this.parentEndpointName = Base64.decode2UTFString(parts[8]);
                } catch (NumberFormatException ignored) {

                }
            }
        }
        return this;
    }

    public boolean isValid() {
        return isValid(HeaderVersion.v2);
    }

    /**
     * Make sure this {@link ContextCarrier} has been initialized.
     *
     * @return true for unbroken {@link ContextCarrier} or no-initialized. Otherwise, false;
     * 判断carrier 是否已经初始化完毕
     */
    boolean isValid(HeaderVersion version) {
        // 只有 v2版本才支持校验
        if (HeaderVersion.v2 == version) {
            return traceSegmentId != null
                && traceSegmentId.isValid()
                && getSpanId() > -1
                && parentServiceInstanceId != DictionaryUtil.nullValue()
                && entryServiceInstanceId != DictionaryUtil.nullValue()
                && !StringUtil.isEmpty(peerHost)
                && primaryDistributedTraceId != null;
        }
        return false;
    }

    public String getEntryEndpointName() {
        return entryEndpointName;
    }

    void setEntryEndpointName(String entryEndpointName) {
        this.entryEndpointName = '#' + entryEndpointName;
    }

    void setEntryEndpointId(int entryOperationId) {
        this.entryEndpointName = entryOperationId + "";
    }

    void setParentEndpointName(String parentEndpointName) {
        this.parentEndpointName = '#' + parentEndpointName;
    }

    void setParentEndpointId(int parentOperationId) {
        this.parentEndpointName = parentOperationId + "";
    }

    public ID getTraceSegmentId() {
        return traceSegmentId;
    }

    public int getSpanId() {
        return spanId;
    }

    void setTraceSegmentId(ID traceSegmentId) {
        this.traceSegmentId = traceSegmentId;
    }

    void setSpanId(int spanId) {
        this.spanId = spanId;
    }

    public int getParentServiceInstanceId() {
        return parentServiceInstanceId;
    }

    void setParentServiceInstanceId(int parentServiceInstanceId) {
        this.parentServiceInstanceId = parentServiceInstanceId;
    }

    public String getPeerHost() {
        return peerHost;
    }

    void setPeerHost(String peerHost) {
        this.peerHost = '#' + peerHost;
    }

    void setPeerId(int peerId) {
        this.peerHost = peerId + "";
    }

    public DistributedTraceId getDistributedTraceId() {
        return primaryDistributedTraceId;
    }

    public void setDistributedTraceIds(List<DistributedTraceId> distributedTraceIds) {
        this.primaryDistributedTraceId = distributedTraceIds.get(0);
    }

    private DistributedTraceId getPrimaryDistributedTraceId() {
        return primaryDistributedTraceId;
    }

    public String getParentEndpointName() {
        return parentEndpointName;
    }

    public int getEntryServiceInstanceId() {
        return entryServiceInstanceId;
    }

    public void setEntryServiceInstanceId(int entryServiceInstanceId) {
        this.entryServiceInstanceId = entryServiceInstanceId;
    }

    public enum HeaderVersion {
        v2
    }
}
