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

package org.apache.skywalking.apm.agent.core.profile;

import org.apache.skywalking.apm.agent.core.context.ids.ID;
import org.apache.skywalking.apm.network.language.profile.ThreadSnapshot;
import org.apache.skywalking.apm.network.language.profile.ThreadStack;

import java.util.List;

/**
 * 线程栈信息的快照对象
 */
public class TracingThreadSnapshot {

    // thread profiler
    private final String taskId;
    /**
     * 当前线程栈在整个链路调用中的  段id
     */
    private final ID traceSegmentId;

    // dump info
    private final int sequence;
    private final long time;
    private final List<String> stackList;

    public TracingThreadSnapshot(String taskId, ID traceSegmentId, int sequence, long time, List<String> stackList) {
        this.taskId = taskId;
        this.traceSegmentId = traceSegmentId;
        this.sequence = sequence;
        this.time = time;
        this.stackList = stackList;
    }

    /**
     * transform to gRPC data
     * 将当前信息转换成 protobuf 格式的数据
     */
    public ThreadSnapshot transform() {
        final ThreadSnapshot.Builder builder = ThreadSnapshot.newBuilder();
        // task id
        builder.setTaskId(taskId);
        // dumped segment id
        builder.setTraceSegmentId(traceSegmentId.transform());
        // dump time
        builder.setTime(time);
        // snapshot dump sequence
        builder.setSequence(sequence);
        // snapshot stack
        final ThreadStack.Builder stackBuilder = ThreadStack.newBuilder();
        for (String codeSign : stackList) {
            stackBuilder.addCodeSignatures(codeSign);
        }
        builder.setStack(stackBuilder);

        return builder.build();
    }

}
