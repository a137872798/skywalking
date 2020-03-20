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

import java.util.Objects;

/**
 * Profile task bean, receive from OAP server
 * 一个task 对应多个线程对某个 操作进行追踪
 */
public class ProfileTask {

    // task id 本次任务id
    private String taskId;

    // monitor first span operation name  在整个调用链路中的首个操作名称  在一段时间内 会有多个线程触发同一个操作 这里是以该操作为入口 统计每条线程的堆栈信息
    private String fistSpanOPName;

    // task duration (minute)  任务持续时间
    private int duration;

    // trace start monitoring time (ms)  至少等待这么长的时间后 才能开启任务
    private int minDurationThreshold;

    // thread dump period (ms)  每间隔多少时间 记录下一次快照信息
    private int threadDumpPeriod;

    // max number of traces monitor on the sniffer  允许采集的最大样品数量
    private int maxSamplingCount;

    // task start time
    private long startTime;

    // task create time
    private long createTime;

    public String getFistSpanOPName() {
        return fistSpanOPName;
    }

    public void setFistSpanOPName(String fistSpanOPName) {
        this.fistSpanOPName = fistSpanOPName;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getMinDurationThreshold() {
        return minDurationThreshold;
    }

    public void setMinDurationThreshold(int minDurationThreshold) {
        this.minDurationThreshold = minDurationThreshold;
    }

    public int getThreadDumpPeriod() {
        return threadDumpPeriod;
    }

    public void setThreadDumpPeriod(int threadDumpPeriod) {
        this.threadDumpPeriod = threadDumpPeriod;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getMaxSamplingCount() {
        return maxSamplingCount;
    }

    public void setMaxSamplingCount(int maxSamplingCount) {
        this.maxSamplingCount = maxSamplingCount;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ProfileTask that = (ProfileTask) o;
        return duration == that.duration && minDurationThreshold == that.minDurationThreshold && threadDumpPeriod == that.threadDumpPeriod && maxSamplingCount == that.maxSamplingCount && startTime == that.startTime && createTime == that.createTime && taskId
            .equals(that.taskId) && fistSpanOPName.equals(that.fistSpanOPName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, fistSpanOPName, duration, minDurationThreshold, threadDumpPeriod, maxSamplingCount, startTime, createTime);
    }
}
