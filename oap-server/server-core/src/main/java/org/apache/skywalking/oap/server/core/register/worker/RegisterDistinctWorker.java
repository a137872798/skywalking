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

package org.apache.skywalking.oap.server.core.register.worker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.consumer.BulkConsumePool;
import org.apache.skywalking.apm.commons.datacarrier.consumer.ConsumerPoolFactory;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.register.RegisterSource;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 相当于在这一层做了 批处理 尽可能的整合数据 并只进行一次处理
 */
public class RegisterDistinctWorker extends AbstractWorker<RegisterSource> {

    private static final Logger logger = LoggerFactory.getLogger(RegisterDistinctWorker.class);

    private final AbstractWorker<RegisterSource> nextWorker;
    private final DataCarrier<RegisterSource> dataCarrier;

    /**
     * value 也选择 source 是因为该容器的目的是 combine source    而针对 registerSource combine就是相关时间戳
     */
    private final Map<RegisterSource, RegisterSource> sources;
    /**
     * 当前接收到的消息总数
     */
    private int messageNum;

    /**
     *
     * @param moduleDefineHolder
     * @param nextWorker  该对象内部实际是一个集群   RegisterRemoteWorker  按照一定的策略选择一个节点去处理数据
     */
    RegisterDistinctWorker(ModuleDefineHolder moduleDefineHolder, AbstractWorker<RegisterSource> nextWorker) {
        super(moduleDefineHolder);
        this.nextWorker = nextWorker;
        this.sources = new HashMap<>();
        this.dataCarrier = new DataCarrier<>(1, 1000);
        String name = "REGISTER_L1";
        int size = BulkConsumePool.Creator.recommendMaxSize() / 8;
        if (size == 0) {
            size = 1;
        }
        BulkConsumePool.Creator creator = new BulkConsumePool.Creator(name, size, 200);
        try {
            ConsumerPoolFactory.INSTANCE.createIfAbsent(name, creator);
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage(), e);
        }
        // 添加消费者
        this.dataCarrier.consume(ConsumerPoolFactory.INSTANCE.get(name), new AggregatorConsumer(this));
    }

    @Override
    public final void in(RegisterSource source) {
        source.resetEndOfBatch();
        dataCarrier.produce(source);
    }

    /**
     * 处理接收到的数据
     * @param source
     */
    private void onWork(RegisterSource source) {
        messageNum++;

        if (!sources.containsKey(source)) {
            sources.put(source, source);
        } else {
            sources.get(source).combine(source);
        }

        if (messageNum >= 1000 || source.isEndOfBatch()) {
            sources.values().forEach(nextWorker::in);
            sources.clear();
            messageNum = 0;
        }
    }

    private class AggregatorConsumer implements IConsumer<RegisterSource> {

        private final RegisterDistinctWorker aggregator;

        private AggregatorConsumer(RegisterDistinctWorker aggregator) {
            this.aggregator = aggregator;
        }

        @Override
        public void init() {
        }

        @Override
        public void consume(List<RegisterSource> sources) {
            Iterator<RegisterSource> sourceIterator = sources.iterator();

            int i = 0;
            while (sourceIterator.hasNext()) {
                RegisterSource source = sourceIterator.next();
                i++;
                if (i == sources.size()) {
                    source.asEndOfBatch();
                }
                aggregator.onWork(source);
            }
        }

        @Override
        public void onError(List<RegisterSource> sources, Throwable t) {
            logger.error(t.getMessage(), t);
        }

        @Override
        public void onExit() {
        }
    }
}
