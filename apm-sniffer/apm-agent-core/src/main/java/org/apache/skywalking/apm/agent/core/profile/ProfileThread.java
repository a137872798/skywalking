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

import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Profile task process thread, dump the executing thread stack.
 * 该对象用于执行 profileTask
 */
public class ProfileThread implements Runnable {

    private static final ILog logger = LogManager.getLogger(ProfileThread.class);

    // profiling task context  执行任务关联的上下文  多个ProfileThread 会关联到一个 context 中
    private final ProfileTaskExecutionContext taskExecutionContext;

    private final ProfileTaskExecutionService profileTaskExecutionService;
    private final ProfileTaskChannelService profileTaskChannelService;

    public ProfileThread(ProfileTaskExecutionContext taskExecutionContext) {
        this.taskExecutionContext = taskExecutionContext;
        profileTaskExecutionService = ServiceManager.INSTANCE.findService(ProfileTaskExecutionService.class);
        profileTaskChannelService = ServiceManager.INSTANCE.findService(ProfileTaskChannelService.class);
    }

    @Override
    public void run() {

        try {
            profiling(taskExecutionContext);
        } catch (InterruptedException e) {
            // ignore interrupted
            // means current task has stopped
        } catch (Exception e) {
            logger.error(e, "Profiling task fail. taskId:{}", taskExecutionContext.getTask().getTaskId());
        } finally {
            // finally stop current profiling task, tell execution service task has stop
            // 结束任务
            profileTaskExecutionService.stopCurrentProfileTask(taskExecutionContext);
        }

    }

    /**
     * start profiling
     * 开始执行任务
     */
    private void profiling(ProfileTaskExecutionContext executionContext) throws InterruptedException {

        // 首先获取任务的转储信息
        int maxSleepPeriod = executionContext.getTask().getThreadDumpPeriod();

        // run loop when current thread still running
        long currentLoopStartTime = -1;
        while (!Thread.currentThread().isInterrupted()) {
            // 每次循环都会更新时间戳
            currentLoopStartTime = System.currentTimeMillis();

            // each all slot  返回所有的任务对象   因为 volatile修饰词 所以总是能读取到最新数据
            AtomicReferenceArray<ThreadProfiler> profilers = executionContext.threadProfilerSlots();
            int profilerCount = profilers.length();
            for (int slot = 0; slot < profilerCount; slot++) {
                ThreadProfiler currentProfiler = profilers.get(slot);
                if (currentProfiler == null) {
                    continue;
                }

                // 判断当前 threadProfiler的状态
                switch (currentProfiler.profilingStatus()) {

                    case READY:
                        // check tracing context running time   检测是否满足启动条件 (也就是要先经过一个最小时间)
                        currentProfiler.startProfilingIfNeed();
                        break;

                    case PROFILING:
                        // dump stack  创建该线程关联的快照对象
                        TracingThreadSnapshot snapshot = currentProfiler.buildSnapshot();
                        if (snapshot != null) {
                            profileTaskChannelService.addProfilingSnapshot(snapshot);
                        } else {
                            // tell execution context current tracing thread dump failed, stop it  不满足条件时 关闭任务
                            executionContext.stopTracingProfile(currentProfiler.tracingContext());
                        }
                        break;

                }
            }

            // sleep to next period
            // if out of period, sleep one period   进行短暂沉睡后进入下一个周期
            long needToSleep = (currentLoopStartTime + maxSleepPeriod) - System.currentTimeMillis();
            needToSleep = needToSleep > 0 ? needToSleep : maxSleepPeriod;
            Thread.sleep(needToSleep);
        }
    }

}
