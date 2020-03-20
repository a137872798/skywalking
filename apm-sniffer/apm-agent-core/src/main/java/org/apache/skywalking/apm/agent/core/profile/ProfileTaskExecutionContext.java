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

import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.context.TracingContext;
import org.apache.skywalking.apm.agent.core.context.ids.ID;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * profile task execution context, it will create on process this profile task
 * 执行profile任务的上下文信息
 */
public class ProfileTaskExecutionContext {

    // task data  一个bean对象 包含一些描述task的信息  一个任务对应多个线程的栈轨迹信息
    private final ProfileTask task;

    // record current profiling count, use this to check has available profile slot   记录当前的profile数量 用于检测slot 是否够用
    private final AtomicInteger currentProfilingCount = new AtomicInteger(0);

    // profiling segment slot  用于存放profile  每个槽对应一个线程   然后profile就是返回该线程的栈轨迹信息
    private volatile AtomicReferenceArray<ThreadProfiler> profilingSegmentSlots;

    // current profiling execution future  当前执行的某个profile任务对应的结果
    private volatile Future profilingFuture;

    // total started profiling tracing context count 总计已经启动了多少任务
    private final AtomicInteger totalStartedProfilingCount = new AtomicInteger(0);

    public ProfileTaskExecutionContext(ProfileTask task) {
        this.task = task;
        profilingSegmentSlots = new AtomicReferenceArray<>(Config.Profile.MAX_PARALLEL);
    }

    /**
     * start profiling this task
     * ProfileThread 线程用于统筹内部所有 ThreadProfiler
     */
    public void startProfiling(ExecutorService executorService) {
        profilingFuture = executorService.submit(new ProfileThread(this));
    }

    /**
     * stop profiling
     * 关闭任务
     */
    public void stopProfiling() {
        if (profilingFuture != null) {
            profilingFuture.cancel(true);
        }
    }

    /**
     * check have available slot to profile and add it
     *
     * @return is add profile success
     * 提交一个 关于某个链路信息的 profile 任务
     */
    public boolean attemptProfiling(TracingContext tracingContext, ID traceSegmentId, String firstSpanOPName) {
        // check has available slot
        final int usingSlotCount = currentProfilingCount.get();
        // 超过了最大并行度 拒绝添加新的 profile任务
        if (usingSlotCount >= Config.Profile.MAX_PARALLEL) {
            return false;
        }

        // check first operation name matches  这里要先匹配 spanOPName  spanOPName 代表针对某个操作
        if (!Objects.equals(task.getFistSpanOPName(), firstSpanOPName)) {
            return false;
        }

        // if out limit started profiling count then stop add profiling  超过了样本最大值
        if (totalStartedProfilingCount.get() > task.getMaxSamplingCount()) {
            return false;
        }

        // try to occupy slot  上面的校验都通过时 增加已使用的slot 数量
        if (!currentProfilingCount.compareAndSet(usingSlotCount, usingSlotCount + 1)) {
            return false;
        }

        final ThreadProfiler threadProfiler = new ThreadProfiler(tracingContext, traceSegmentId, Thread.currentThread(), this);
        int slotLength = profilingSegmentSlots.length();
        // 使用这种方式 也就是无锁实现
        for (int slot = 0; slot < slotLength; slot++) {
            if (profilingSegmentSlots.compareAndSet(slot, null, threadProfiler)) {
                break;
            }
        }
        return true;
    }

    /**
     * profiling recheck
     * 如果context 已经在检测中不需要处理
     */
    public boolean profilingRecheck(TracingContext tracingContext, ID traceSegmentId, String firstSpanOPName) {
        // if started, keep profiling
        if (tracingContext.isProfiling()) {
            return true;
        }

        // 否则添加一个 profile 任务到数组中
        return attemptProfiling(tracingContext, traceSegmentId, firstSpanOPName);
    }

    /**
     * find tracing context and clear on slot
     * 关闭某个任务   这里一个链路上下文对应一个 profileSlot  那么看来该对象可以同时监控多个链路了
     */
    public void stopTracingProfile(TracingContext tracingContext) {
        // find current tracingContext and clear it
        int slotLength = profilingSegmentSlots.length();
        for (int slot = 0; slot < slotLength; slot++) {
            ThreadProfiler currentProfiler = profilingSegmentSlots.get(slot);
            if (currentProfiler != null && currentProfiler.matches(tracingContext)) {
                profilingSegmentSlots.set(slot, null);

                // setting stop running
                currentProfiler.stopProfiling();
                currentProfilingCount.addAndGet(-1);
                break;
            }
        }
    }

    public ProfileTask getTask() {
        return task;
    }

    public AtomicReferenceArray<ThreadProfiler> threadProfilerSlots() {
        return profilingSegmentSlots;
    }

    public boolean isStartProfileable() {
        // check is out of max sampling count check
        return totalStartedProfilingCount.incrementAndGet() <= task.getMaxSamplingCount();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ProfileTaskExecutionContext that = (ProfileTaskExecutionContext) o;
        return Objects.equals(task, that.task);
    }

    @Override
    public int hashCode() {
        return Objects.hash(task);
    }
}
