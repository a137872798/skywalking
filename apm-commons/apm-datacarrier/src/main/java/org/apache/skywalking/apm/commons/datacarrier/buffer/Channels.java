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

package org.apache.skywalking.apm.commons.datacarrier.buffer;

import org.apache.skywalking.apm.commons.datacarrier.partition.IDataPartitioner;

/**
 * Channels of Buffer It contains all buffer data which belongs to this channel. It supports several strategy when
 * buffer is full. The Default is BLOCKING <p> Created by wusheng on 2016/10/25.
 */
public class Channels<T> {
    /**
     * 阻塞容器数组 支持当无法填充时 阻塞当前线程
     */
    private final QueueBuffer<T>[] bufferChannels;
    /**
     * 数据分区器 通过该对象判定 某个数据应该存放在哪里
     */
    private IDataPartitioner<T> dataPartitioner;
    private BufferStrategy strategy;
    // 可存储数据的总大小
    private final long size;

    /**
     *
     * @param channelSize  一共有多少个 buffer
     * @param bufferSize   指定了 一共有多少个slot 可存放数据
     * @param partitioner
     * @param strategy
     */
    public Channels(int channelSize, int bufferSize, IDataPartitioner<T> partitioner, BufferStrategy strategy) {
        this.dataPartitioner = partitioner;
        this.strategy = strategy;
        bufferChannels = new QueueBuffer[channelSize];
        for (int i = 0; i < channelSize; i++) {
            if (BufferStrategy.BLOCKING.equals(strategy)) {
                bufferChannels[i] = new ArrayBlockingQueueBuffer<T>(bufferSize, strategy);
            } else {
                bufferChannels[i] = new Buffer<T>(bufferSize, strategy);
            }
        }
        size = channelSize * bufferSize;
    }

    /**
     * 尝试存储数据
     * @param data
     * @return
     */
    public boolean save(T data) {
        // 首先判断应该存放在哪个 buffer
        int index = dataPartitioner.partition(bufferChannels.length, data);
        int retryCountDown = 1;
        if (BufferStrategy.IF_POSSIBLE.equals(strategy)) {
            int maxRetryCount = dataPartitioner.maxRetryCount();
            if (maxRetryCount > 1) {
                retryCountDown = maxRetryCount;
            }
        }
        // 尝试添加数据
        for (; retryCountDown > 0; retryCountDown--) {
            if (bufferChannels[index].save(data)) {
                return true;
            }
        }
        return false;
    }

    public void setPartitioner(IDataPartitioner<T> dataPartitioner) {
        this.dataPartitioner = dataPartitioner;
    }

    /**
     * override the strategy at runtime. Notice, this will override several channels one by one. So, when running
     * setStrategy, each channel may use different BufferStrategy
     */
    public void setStrategy(BufferStrategy strategy) {
        for (QueueBuffer<T> buffer : bufferChannels) {
            buffer.setStrategy(strategy);
        }
    }

    /**
     * get channelSize
     */
    public int getChannelSize() {
        return this.bufferChannels.length;
    }

    public long size() {
        return size;
    }

    public QueueBuffer<T> getBuffer(int index) {
        return this.bufferChannels[index];
    }
}
