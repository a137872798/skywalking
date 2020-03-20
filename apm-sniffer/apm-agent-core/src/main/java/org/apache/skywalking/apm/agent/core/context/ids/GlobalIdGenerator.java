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

import java.util.Random;
import org.apache.skywalking.apm.agent.core.conf.RemoteDownstreamConfig;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;

/**
 * 全局id 生成器
 */
public final class GlobalIdGenerator {
    private static final ThreadLocal<IDContext> THREAD_ID_SEQUENCE = ThreadLocal.withInitial(
        () -> new IDContext(System.currentTimeMillis(), (short) 0));

    private GlobalIdGenerator() {
    }

    /**
     * Generate a new id, combined by three long numbers.
     * <p>
     * The first one represents application instance id. (most likely just an integer value, would be helpful in
     * protobuf)
     * <p>
     * The second one represents thread id. (most likely just an integer value, would be helpful in protobuf)
     * <p>
     * The third one also has two parts, 1) a timestamp, measured in milliseconds 2) a seq, in current thread, between
     * 0(included) and 9999(included)
     * <p>
     * Notice, a long costs 8 bytes, three longs cost 24 bytes. And at the same time, a char costs 2 bytes. So
     * sky-walking's old global and segment id like this: "S.1490097253214.-866187727.57515.1.1" which costs at least 72
     * bytes.
     *
     * @return an array contains three long numbers, which represents a unique id.
     * 生成全局id  每个全局id 包含3个部分
     */
    public static ID generate() {
        // 如果此时 service_instance 还没有设置 抛出异常
        if (RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID == DictionaryUtil.nullValue()) {
            throw new IllegalStateException();
        }
        // 获取当前线程对应的 idContext
        IDContext context = THREAD_ID_SEQUENCE.get();

        // 通过3个部分生成唯一id
        return new ID(
            RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID,
            Thread.currentThread().getId(),
            context.nextSeq()
        );
    }

    private static class IDContext {
        /**
         * 在初始化时 为当前时间戳
         */
        private long lastTimestamp;
        /**
         * 默认为0
         */
        private short threadSeq;

        // Just for considering time-shift-back only.
        private long runRandomTimestamp;
        private int lastRandomValue;
        private Random random;

        private IDContext(long lastTimestamp, short threadSeq) {
            this.lastTimestamp = lastTimestamp;
            this.threadSeq = threadSeq;
        }

        /**
         * 唯一id 除了以 client信息作为前缀外 之后就是 时间戳 + 一个递增标识的组合
         * @return
         */
        private long nextSeq() {
            return timestamp() * 10000 + nextThreadSeq();
        }

        /**
         * 返回当前时间戳
         * @return
         */
        private long timestamp() {
            long currentTimeMillis = System.currentTimeMillis();

            // 这里是属于特殊情况 什么时间震荡
            if (currentTimeMillis < lastTimestamp) {
                // Just for considering time-shift-back by Ops or OS. @hanahmily 's suggestion.
                if (random == null) {
                    random = new Random();
                }
                if (runRandomTimestamp != currentTimeMillis) {
                    // 默认返回任意一个int值
                    lastRandomValue = random.nextInt();
                    runRandomTimestamp = currentTimeMillis;
                }
                return lastRandomValue;
            } else {
                lastTimestamp = currentTimeMillis;
                return lastTimestamp;
            }
        }

        /**
         * 调用1000次 回归到0
         * @return
         */
        private short nextThreadSeq() {
            if (threadSeq == 10000) {
                threadSeq = 0;
            }
            return threadSeq++;
        }
    }
}
