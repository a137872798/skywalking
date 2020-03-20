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

import java.util.List;
import org.apache.skywalking.apm.commons.datacarrier.common.AtomicRangeInteger;

/**
 * Self implementation ring queue.
 * 默认存储数据的对象
 * 这个类在并发环境下是怎么确保可见性的???
 */
public class Buffer<T> implements QueueBuffer<T> {
    private final Object[] buffer;

    /**
     * 当空间不足时 选择阻塞等待数据释放 或者 尽可能的填充数据
     */
    private BufferStrategy strategy;
    private AtomicRangeInteger index;

    Buffer(int bufferSize, BufferStrategy strategy) {
        // 根据初始长度来生成数组
        buffer = new Object[bufferSize];
        this.strategy = strategy;
        index = new AtomicRangeInteger(0, bufferSize);
    }

    public void setStrategy(BufferStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * 存储数据
     * @param data to add.
     * @return
     */
    public boolean save(T data) {
        // index 就是一个会在 0~bufferSize 之间变化的值
        int i = index.getAndIncrement();
        // 发现想要占用的位置已经设置了别的对象
        if (buffer[i] != null) {
            switch (strategy) {
                // 阻塞直到该下标对应的数据被置空
                case BLOCKING:
                    while (buffer[i] != null) {
                        try {
                            Thread.sleep(1L);
                        } catch (InterruptedException e) {
                        }
                    }
                    break;
                // 否则返回false
                case IF_POSSIBLE:
                    return false;
                default:
            }
        }
        // 默认情况选择覆盖
        buffer[i] = data;
        return true;
    }

    public int getBufferSize() {
        return buffer.length;
    }

    public void obtain(List<T> consumeList) {
        this.obtain(consumeList, 0, buffer.length);
    }

    /**
     * 将内部数据移动到list中
     * @param consumeList
     * @param start
     * @param end
     */
    void obtain(List<T> consumeList, int start, int end) {
        for (int i = start; i < end; i++) {
            if (buffer[i] != null) {
                consumeList.add((T) buffer[i]);
                buffer[i] = null;
            }
        }
    }

}
