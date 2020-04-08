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

package org.apache.skywalking.apm.agent.core.context.ids;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DistributedTraceIds {
    private LinkedList<DistributedTraceId> relatedGlobalTraces;

    public DistributedTraceIds() {
        relatedGlobalTraces = new LinkedList<DistributedTraceId>();
    }

    /**
     * 返回一组关联的链路id
     * @return
     */
    public List<DistributedTraceId> getRelatedGlobalTraces() {
        return Collections.unmodifiableList(relatedGlobalTraces);
    }

    /**
     * 当跨进程时 追加上新的链路id
     * @param distributedTraceId
     */
    public void append(DistributedTraceId distributedTraceId) {
        // 这样就只有发起者 是包含NewDistributedTraceId 的 其余间接被触发的 链路segment 都依赖于之前的 segment 那么将他们的id 设置进来时 就会将 NewDistributedTraceId 移除
        if (relatedGlobalTraces.size() > 0 && relatedGlobalTraces.getFirst() instanceof NewDistributedTraceId) {
            relatedGlobalTraces.removeFirst();
        }
        if (!relatedGlobalTraces.contains(distributedTraceId)) {
            relatedGlobalTraces.add(distributedTraceId);
        }
    }
}
