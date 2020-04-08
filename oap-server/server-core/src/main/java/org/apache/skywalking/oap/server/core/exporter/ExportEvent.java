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

package org.apache.skywalking.oap.server.core.exporter;

import lombok.Getter;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;

/**
 * The event for exporter {@link MetricValuesExportService} implementation processes. {@link #metrics} should not be
 * changed in any case.
 * 代表生成了一个 Export事件
 */
@Getter
public class ExportEvent {
    /**
     * Fields of this should not be changed in any case.
     * 对应测量到的数据
     */
    private Metrics metrics;
    /**
     * 本次事件类型
     */
    private EventType type;

    public ExportEvent(Metrics metrics, EventType type) {
        this.metrics = metrics;
        this.type = type;
    }

    public enum EventType {
        /**
         * The metrics aggregated in this bulk, not include the existing persistent data.
         * 代表本次的变化 而没有整合之前的数据
         */
        INCREMENT,
        /**
         * Final result of the metrics at this moment.
         * 基于原先的数据在本次变化之后的最终值
         */
        TOTAL
    }
}
