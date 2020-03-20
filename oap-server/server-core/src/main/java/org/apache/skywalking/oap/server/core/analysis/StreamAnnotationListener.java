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

package org.apache.skywalking.oap.server.core.analysis;

import java.lang.annotation.Annotation;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.worker.MetricsStreamProcessor;
import org.apache.skywalking.oap.server.core.analysis.worker.NoneStreamingProcessor;
import org.apache.skywalking.oap.server.core.analysis.worker.RecordStreamProcessor;
import org.apache.skywalking.oap.server.core.analysis.worker.TopNStreamProcessor;
import org.apache.skywalking.oap.server.core.annotation.AnnotationListener;
import org.apache.skywalking.oap.server.core.register.worker.InventoryStreamProcessor;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;

/**
 * Stream annotation listener, process the class with {@link Stream} annotation.
 * 该对象就是用于监听 @Stream 注解的
 */
public class StreamAnnotationListener implements AnnotationListener {

    /**
     * 对应 moduleManager  用于统一加载所有的module 以及加载配置进行装配 以及创建provider 并启动
     */
    private final ModuleDefineHolder moduleDefineHolder;

    public StreamAnnotationListener(ModuleDefineHolder moduleDefineHolder) {
        this.moduleDefineHolder = moduleDefineHolder;
    }

    @Override
    public Class<? extends Annotation> annotation() {
        return Stream.class;
    }


    /**
     * 当感知到某个类 携带 @Stream 时
     * @param aClass
     */
    @SuppressWarnings("unchecked")
    @Override
    public void notify(Class aClass) {
        if (aClass.isAnnotationPresent(Stream.class)) {
            Stream stream = (Stream) aClass.getAnnotation(Stream.class);

            // 处理器本身是单例对象 内部维护了一些映射关系 针对不同的数据源 走不同的processor
            if (stream.processor().equals(InventoryStreamProcessor.class)) {
                // 添加一组映射关系     这里是处理 RegisterSource 类型的数据
                InventoryStreamProcessor.getInstance().create(moduleDefineHolder, stream, aClass);
            } else if (stream.processor().equals(RecordStreamProcessor.class)) {
                RecordStreamProcessor.getInstance().create(moduleDefineHolder, stream, aClass);
            } else if (stream.processor().equals(MetricsStreamProcessor.class)) {
                MetricsStreamProcessor.getInstance().create(moduleDefineHolder, stream, aClass);
            } else if (stream.processor().equals(TopNStreamProcessor.class)) {
                TopNStreamProcessor.getInstance().create(moduleDefineHolder, stream, aClass);
            } else if (stream.processor().equals(NoneStreamingProcessor.class)) {
                NoneStreamingProcessor.getInstance().create(moduleDefineHolder, stream, aClass);
            } else {
                throw new UnexpectedException("Unknown stream processor.");
            }
        } else {
            throw new UnexpectedException("Stream annotation listener could only parse the class present stream annotation.");
        }
    }
}
