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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.context.TracingContext;
import org.apache.skywalking.apm.agent.core.context.TracingThreadListener;
import org.apache.skywalking.apm.agent.core.context.ids.ID;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.network.constants.ProfileConstants;
import org.apache.skywalking.apm.util.StringUtil;

/**
 * Profile task executor, use {@link #addProfileTask(ProfileTask)} to add a new profile task.
 * 该对象相当于是 整个profile体系的入口
 */
@DefaultImplementor
public class ProfileTaskExecutionService implements BootService, TracingThreadListener {

    private static final ILog logger = LogManager.getLogger(ProfileTaskExecutionService.class);

    // add a schedule while waiting for the task to start or finish
    // 内部使用一个定时器来执行 profile任务
    private final static ScheduledExecutorService PROFILE_TASK_SCHEDULE = Executors.newSingleThreadScheduledExecutor(
        new DefaultNamedThreadFactory("PROFILE-TASK-SCHEDULE"));

    // last command create time, use to next query task list
    // 上一次执行任务的时间
    private volatile long lastCommandCreateTime = -1;

    // current processing profile task context
    // 当前执行profile任务的上下文信息
    private final AtomicReference<ProfileTaskExecutionContext> taskExecutionContext = new AtomicReference<>();

    // profile executor thread pool, only running one thread   用于执行 ProfileThread的执行器
    private final static ExecutorService PROFILE_EXECUTOR = Executors.newSingleThreadExecutor(
        new DefaultNamedThreadFactory("PROFILING-TASK"));

    // profile task list, include running and waiting running tasks
    // 存放一组任务列表  要注意 每个task 对应一组线程 以及他们的栈轨迹信息 每个task的统计任务使用相同的配置
    private final List<ProfileTask> profileTaskList = Collections.synchronizedList(new LinkedList<>());

    /**
     * add profile task from OAP
     * 添加一个新的统计任务
     */
    public void addProfileTask(ProfileTask task) {
        // update last command create time  更新最后的提交时间
        if (task.getCreateTime() > lastCommandCreateTime) {
            lastCommandCreateTime = task.getCreateTime();
        }

        // check profile task limit  检测能否正常添加
        final CheckResult dataError = checkProfileTaskSuccess(task);
        if (!dataError.isSuccess()) {
            logger.warn(
                "check command error, cannot process this profile task. reason: {}", dataError.getErrorReason());
            return;
        }

        // add task to list  校验成功时将任务添加到列表中
        profileTaskList.add(task);

        // schedule to start task  还有多久时间才开始执行
        long timeToProcessMills = task.getStartTime() - System.currentTimeMillis();
        // 延迟执行该任务
        PROFILE_TASK_SCHEDULE.schedule(() -> processProfileTask(task), timeToProcessMills, TimeUnit.MILLISECONDS);
    }

    /**
     * check and add {@link TracingContext} profiling
     * 往当前执行的某个 task 对应的context 中插入一个 ThreadProfiler 对象
     */
    public boolean addProfiling(TracingContext tracingContext, ID traceSegmentId, String firstSpanOPName) {
        // get current profiling task, check need profiling
        final ProfileTaskExecutionContext executionContext = taskExecutionContext.get();
        if (executionContext == null) {
            return false;
        }

        return executionContext.attemptProfiling(tracingContext, traceSegmentId, firstSpanOPName);
    }

    /**
     * Re-check current trace need profiling, in case that third-party plugins change the operation name.
     * 重新检查当前链路对应的 profile 任务
     */
    public boolean profilingRecheck(TracingContext tracingContext, ID traceSegmentId, String firstSpanOPName) {
        // get current profiling task, check need profiling
        final ProfileTaskExecutionContext executionContext = taskExecutionContext.get();
        if (executionContext == null) {
            return false;
        }

        // 如果 tracingContext 还未处在 profiling 状态 那么会间接触发 attemptProfiling
        return executionContext.profilingRecheck(tracingContext, traceSegmentId, firstSpanOPName);
    }

    /**
     * active the selected profile task to execution task, and start a removal task for it.
     * 当为该 executionService 添加一个task 后 会在该任务的 startTime 时触发该方法
     */
    private synchronized void processProfileTask(ProfileTask task) {
        // make sure prev profile task already stopped  首先停止当前正在执行的task
        stopCurrentProfileTask(taskExecutionContext.get());

        // make stop task schedule and task context  生成对应的上下文对象 并在一定延时后关闭
        final ProfileTaskExecutionContext currentStartedTaskContext = new ProfileTaskExecutionContext(task);
        taskExecutionContext.set(currentStartedTaskContext);

        // start profiling this task  执行 ProfileThread
        currentStartedTaskContext.startProfiling(PROFILE_EXECUTOR);

        PROFILE_TASK_SCHEDULE.schedule(
            () -> stopCurrentProfileTask(currentStartedTaskContext), task.getDuration(), TimeUnit.MINUTES);
    }

    /**
     * stop profile task, remove context data
     * 停止当前执行的任务
     */
    synchronized void stopCurrentProfileTask(ProfileTaskExecutionContext needToStop) {
        // stop same context only
        if (needToStop == null || !taskExecutionContext.compareAndSet(needToStop, null)) {
            return;
        }

        // current execution stop running
        needToStop.stopProfiling();

        // remove task
        profileTaskList.remove(needToStop.getTask());

        // notify profiling task has finished
        ServiceManager.INSTANCE.findService(ProfileTaskChannelService.class)
                               .notifyProfileTaskFinish(needToStop.getTask());
    }

    @Override
    public void prepare() {
    }

    @Override
    public void boot() {
    }

    @Override
    public void onComplete() {
        // add trace finish notification 维护全局范围内的监听器
        TracingContext.TracingThreadListenerManager.add(this);
    }

    @Override
    public void shutdown() {
        // remove trace listener  当该对象终止时 从全局变量中移除监听器
        TracingContext.TracingThreadListenerManager.remove(this);

        PROFILE_TASK_SCHEDULE.shutdown();

        PROFILE_EXECUTOR.shutdown();
    }

    /**
     * 获取上次添加 ProfileTask 的时间
     * @return
     */
    public long getLastCommandCreateTime() {
        return lastCommandCreateTime;
    }

    /**
     * check profile task data success, make the re-check, prevent receiving wrong data from database or OAP
     * 检测能否正常添加task
     */
    private CheckResult checkProfileTaskSuccess(ProfileTask task) {
        // endpoint name  如果task 没有设置 spanOPName 那么不允许添加任务
        if (StringUtil.isEmpty(task.getFistSpanOPName())) {
            return new CheckResult(false, "endpoint name cannot be empty");
        }

        // 检验参数是否合法

        // duration
        if (task.getDuration() < ProfileConstants.TASK_DURATION_MIN_MINUTE) {
            return new CheckResult(
                false, "monitor duration must greater than " + ProfileConstants.TASK_DURATION_MIN_MINUTE + " minutes");
        }
        if (task.getDuration() > ProfileConstants.TASK_DURATION_MAX_MINUTE) {
            return new CheckResult(
                false,
                "The duration of the monitoring task cannot be greater than " + ProfileConstants.TASK_DURATION_MAX_MINUTE + " minutes"
            );
        }

        // min duration threshold
        if (task.getMinDurationThreshold() < 0) {
            return new CheckResult(false, "min duration threshold must greater than or equals zero");
        }

        // dump period
        if (task.getThreadDumpPeriod() < ProfileConstants.TASK_DUMP_PERIOD_MIN_MILLIS) {
            return new CheckResult(
                false,
                "dump period must be greater than or equals " + ProfileConstants.TASK_DUMP_PERIOD_MIN_MILLIS + " milliseconds"
            );
        }

        // max sampling count
        if (task.getMaxSamplingCount() <= 0) {
            return new CheckResult(false, "max sampling count must greater than zero");
        }
        if (task.getMaxSamplingCount() >= ProfileConstants.TASK_MAX_SAMPLING_COUNT) {
            return new CheckResult(
                false, "max sampling count must less than " + ProfileConstants.TASK_MAX_SAMPLING_COUNT);
        }

        // check task queue, check only one task in a certain time   计算该task 的结束时间戳
        long taskProcessFinishTime = calcProfileTaskFinishTime(task);
        // 代表该 task 所属的时间段被完全包含在 本次传入的task之内
        for (ProfileTask profileTask : profileTaskList) {

            // if the end time of the task to be added is during the execution of any data, means is a error data
            if (taskProcessFinishTime >= profileTask.getStartTime() && taskProcessFinishTime <= calcProfileTaskFinishTime(
                profileTask)) {
                return new CheckResult(
                    false,
                    "there already have processing task in time range, could not add a new task again. processing task monitor endpoint name: "
                        + profileTask.getFistSpanOPName()
                );
            }
        }

        return new CheckResult(true, null);
    }

    /**
     * 计算某个任务的结束时间
     * @param task
     * @return
     */
    private long calcProfileTaskFinishTime(ProfileTask task) {
        return task.getStartTime() + TimeUnit.MINUTES.toMillis(task.getDuration());
    }

    /**
     * 钩子函数 在监听到某个 context 结束时触发   关闭用于监控该 TracingContext的 ThreadProfiler
     * @param tracingContext
     */
    @Override
    public void afterMainThreadFinish(TracingContext tracingContext) {
        if (tracingContext.isProfiling()) {
            // stop profiling tracing context
            ProfileTaskExecutionContext currentExecutionContext = taskExecutionContext.get();
            if (currentExecutionContext != null) {
                currentExecutionContext.stopTracingProfile(tracingContext);
            }
        }
    }

    /**
     * check profile task is processable
     * 检测能否添加task 的结果
     */
    private static class CheckResult {
        private boolean success;
        private String errorReason;

        public CheckResult(boolean success, String errorReason) {
            this.success = success;
            this.errorReason = errorReason;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorReason() {
            return errorReason;
        }
    }
}
