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

import com.google.common.base.Objects;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.TracingContext;
import org.apache.skywalking.apm.agent.core.context.ids.ID;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 该对象包含了 执行 profileTask 相关的信息 比如context thread
 */
public class ThreadProfiler {

    // current tracing context
    private final TracingContext tracingContext;
    // current tracing segment id  当前链路段id
    private final ID traceSegmentId;
    // need to profiling thread  代表链路对应的那条线程
    private final Thread profilingThread;
    // profiling execution context
    private final ProfileTaskExecutionContext executionContext;

    // profiling time   开始时间 以及最长持续时间
    private long profilingStartTime;
    private long profilingMaxTimeMills;

    // after min duration threshold check, it will start dump  默认情况处于 Ready
    private ProfilingStatus profilingStatus = ProfilingStatus.READY;
    // thread dump sequence
    private int dumpSequence = 0;

    /**
     * 工作代码 和 收集profile信息的代码 运行在不同的线程  当每个工作代码需要统计 profile信息时 生成一个 ThreadProfile对象并添加到 ProfileTaskExecutionContext 中  此时 profilingThread 绑定到工作线程
     * @param tracingContext
     * @param traceSegmentId
     * @param profilingThread
     * @param executionContext
     */
    public ThreadProfiler(TracingContext tracingContext, ID traceSegmentId, Thread profilingThread,
        ProfileTaskExecutionContext executionContext) {
        this.tracingContext = tracingContext;
        this.traceSegmentId = traceSegmentId;
        this.profilingThread = profilingThread;
        this.executionContext = executionContext;
        this.profilingMaxTimeMills = TimeUnit.MINUTES.toMillis(Config.Profile.MAX_DURATION);
    }

    /**
     * If tracing start time greater than {@link ProfileTask#getMinDurationThreshold()}, then start to profiling trace
     * 当上下文的创建时间距今 已经超过了  minDurationThreshold 的时候 就可以标记启动
     */
    public void startProfilingIfNeed() {
        if (System.currentTimeMillis() - tracingContext.createTime() > executionContext.getTask()
                                                                                       .getMinDurationThreshold()) {
            this.profilingStartTime = System.currentTimeMillis();
            this.profilingStatus = ProfilingStatus.PROFILING;
        }
    }

    /**
     * Stop profiling status
     * 标记当前状态为关闭
     */
    public void stopProfiling() {
        this.profilingStatus = ProfilingStatus.STOPPED;
    }

    /**
     * dump tracing thread and build thread snapshot
     *
     * @return snapshot, if null means dump snapshot error, should stop it
     * 生成快照对象
     */
    public TracingThreadSnapshot buildSnapshot() {
        // 确保还在时间段内
        if (!isProfilingContinuable()) {
            return null;
        }

        long currentTime = System.currentTimeMillis();
        // dump thread
        StackTraceElement[] stackTrace;
        try {
            // 获取链路线程的栈轨迹信息
            stackTrace = profilingThread.getStackTrace();

            // stack depth is zero, means thread is already run finished  此时没有堆栈信息直接返回null
            if (stackTrace.length == 0) {
                return null;
            }
        } catch (Exception e) {
            // dump error ignore and make this profiler stop
            return null;
        }

        // if is first dump, check is can start profiling
        // 代表首次尝试生成样本时 发现超过了 允许的样本最大值 那么返回null 后 该对象会被关闭
        if (dumpSequence == 0 && (!executionContext.isStartProfileable())) {
            return null;
        }

        // 从当前栈轨迹信息 和最大深度中取 较小的值
        int dumpElementCount = Math.min(stackTrace.length, Config.Profile.DUMP_MAX_STACK_DEPTH);

        // use inverted order, because thread dump is start with bottom  倒序保存结果
        final ArrayList<String> stackList = new ArrayList<>(dumpElementCount);
        for (int i = dumpElementCount - 1; i >= 0; i--) {
            stackList.add(buildStackElementCodeSignature(stackTrace[i]));
        }

        String taskId = executionContext.getTask().getTaskId();
        return new TracingThreadSnapshot(taskId, traceSegmentId, dumpSequence++, currentTime, stackList);
    }

    /**
     * build thread stack element code signature
     *
     * @return code sign: className.methodName:lineNumber
     * 生成格式化信息
     */
    private String buildStackElementCodeSignature(StackTraceElement element) {
        return element.getClassName() + "." + element.getMethodName() + ":" + element.getLineNumber();
    }

    /**
     * matches profiling tracing context
     */
    public boolean matches(TracingContext context) {
        // match trace id  匹配全局链路中 segment 的全局id
        return Objects.equal(context.getReadableGlobalTraceId(), tracingContext.getReadableGlobalTraceId());
    }

    /**
     * check whether profiling should continue
     *
     * @return if true means this thread profiling is continuable
     */
    private boolean isProfilingContinuable() {
        return System.currentTimeMillis() - profilingStartTime < profilingMaxTimeMills;
    }

    public TracingContext tracingContext() {
        return tracingContext;
    }

    public ProfilingStatus profilingStatus() {
        return profilingStatus;
    }

}
