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

package org.apache.skywalking.apm.agent.core.context;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.RemoteDownstreamConfig;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.TraceSegment;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;
import org.apache.skywalking.apm.util.StringUtil;

import static org.apache.skywalking.apm.agent.core.conf.Config.Agent.OPERATION_NAME_THRESHOLD;

/**
 * {@link ContextManager} controls the whole context of {@link TraceSegment}. Any {@link TraceSegment} relates to
 * single-thread, so this context use {@link ThreadLocal} to maintain the context, and make sure, since a {@link
 * TraceSegment} starts, all ChildOf spans are in the same context. <p> What is 'ChildOf'?
 * https://github.com/opentracing/specification/blob/master/specification.md#references-between-spans
 *
 * <p> Also, {@link ContextManager} delegates to all {@link AbstractTracerContext}'s major methods.
 * 用于管理链路的对象  默认情况下第二个启动
 */
public class ContextManager implements BootService {
    private static final ILog logger = LogManager.getLogger(ContextManager.class);
    /**
     * 链路信息绑定在本地线程
     */
    private static ThreadLocal<AbstractTracerContext> CONTEXT = new ThreadLocal<AbstractTracerContext>();
    /**
     * 运行时上下文信息  就是一个map 内部保存了一些属性
     */
    private static ThreadLocal<RuntimeContext> RUNTIME_CONTEXT = new ThreadLocal<RuntimeContext>();
    /**
     * 该对象负责创建上下文
     */
    private static ContextManagerExtendService EXTEND_SERVICE;

    /**
     * 根据某个操作名创建上下文对象
     * @param operationName
     * @param forceSampling
     * @return
     */
    private static AbstractTracerContext getOrCreate(String operationName, boolean forceSampling) {
        // 先判断当前线程是否包含上下文
        AbstractTracerContext context = CONTEXT.get();
        if (context == null) {
            // 如果没有设置操作名 返回一个空的上下文
            if (StringUtil.isEmpty(operationName)) {
                if (logger.isDebugEnable()) {
                    logger.debug("No operation name, ignore this trace.");
                }
                // 该对象还是存在 depth  信息的
                context = new IgnoredTracerContext();
            } else {
                // 当设置了操作名时 首先要求 服务id 和 实例id 不为空 也就是本节点已经注册在 skywalking 上了
                if (RemoteDownstreamConfig.Agent.SERVICE_ID != DictionaryUtil.nullValue()
                    && RemoteDownstreamConfig.Agent.SERVICE_INSTANCE_ID != DictionaryUtil.nullValue()) {
                    if (EXTEND_SERVICE == null) {
                        EXTEND_SERVICE = ServiceManager.INSTANCE.findService(ContextManagerExtendService.class);
                    }
                    // 创建上下文对象
                    context = EXTEND_SERVICE.createTraceContext(operationName, forceSampling);
                } else {
                    /*
                     * Can't register to collector, no need to trace anything.
                     * 当没有注册服务信息时返回一个空的上下文
                     */
                    context = new IgnoredTracerContext();
                }
            }
            CONTEXT.set(context);
        }
        // 如果当前已经存在上下文的情况 那么直接返回上下文
        return context;
    }

    /**
     * 获取当前线程绑定的上下文  当接收到 dubbo生成的结果时 会填充该值
     * @return
     */
    private static AbstractTracerContext get() {
        return CONTEXT.get();
    }

    /**
     * @return the first global trace id if needEnhance. Otherwise, "N/A".
     * 获取本上下文 在全局范围的链路id
     */
    public static String getGlobalTraceId() {
        AbstractTracerContext segment = CONTEXT.get();
        if (segment == null) {
            return "N/A";
        } else {
            return segment.getReadableGlobalTraceId();
        }
    }

    /**
     * 比如 作为dubbo 的提供者端 接收到一个请求就会触发该方法
     * @param operationName  描述本次操作信息 比如调用了什么方法 路径是什么
     * @param carrier  该对象负责存储数据
     * @return
     */
    public static AbstractSpan createEntrySpan(String operationName, ContextCarrier carrier) {
        AbstractSpan span;
        AbstractTracerContext context;
        // 如果 操作名过长 则进行裁剪
        operationName = StringUtil.cut(operationName, OPERATION_NAME_THRESHOLD);
        // 这里代表 carrier 携带了数据   dubbo提供者一端会收到 消费者设置进去的数据 然后用来填充 carrier
        if (carrier != null && carrier.isValid()) {
            SamplingService samplingService = ServiceManager.INSTANCE.findService(SamplingService.class);
            // 增加样品数量 同时samplingService 每过3秒就会重置样品数量
            samplingService.forceSampled();
            // 使用操作名去创建一个上下文
            context = getOrCreate(operationName, true);
            // 当创建完上下文后 (也可能是从当前线程拿到的)  然后用上下文对象去创建一个 entrySpan
            span = context.createEntrySpan(operationName);
            // 将本次span 和 segment关联到 carrier携带的segment上
            context.extract(carrier);
        } else {
            // 如果 carrier 为空 或者 内部数据无效 那么创建一个新的context    skywalking 和 seats 不一样 没有传递本地线程变量  这里会创建一个全新的上下文
            context = getOrCreate(operationName, false);
            // 创建 entrySpan
            span = context.createEntrySpan(operationName);
        }
        return span;
    }

    /**
     * 创建本地的 span
     * @param operationName
     * @return
     */
    public static AbstractSpan createLocalSpan(String operationName) {
        operationName = StringUtil.cut(operationName, OPERATION_NAME_THRESHOLD);
        AbstractTracerContext context = getOrCreate(operationName, false);
        return context.createLocalSpan(operationName);
    }

    /**
     * 创建 exitSpan  对应dubbo 就是当本节点作为一个消费者 往外发送请求时 会创建一个 exitSpan对象
     * @param operationName  本次操作名称 一般就是描述本次消费者调用的是哪个provider 地址
     * @param carrier 该对象用于存放数据
     * @param remotePeer  provider 地址
     * @return
     */
    public static AbstractSpan createExitSpan(String operationName, ContextCarrier carrier, String remotePeer) {
        if (carrier == null) {
            throw new IllegalArgumentException("ContextCarrier can't be null.");
        }
        // 如果信息过长 截取至500
        operationName = StringUtil.cut(operationName, OPERATION_NAME_THRESHOLD);
        // 这里生成链路上下文对象  context相当于是桥梁 再一次完整链路中 每个牵扯到的节点都会作为一个 segment 内部的 entrySpan exitSpan 分别对应 发出一个请求和接收到一个结果
        // 什么是一条完整的链路 指的就是从 第一个跨进程调用开始到结束  比如 某个Dubbo 方法 只要发起调用并收到结果 也就是一次完整链路了 至于内部又涉及到多少服务就是要记录的点
        // 而当进入下一个dubbo方法时 实际上已经跟之前的链路无关了
        AbstractTracerContext context = getOrCreate(operationName, false);
        // context 相当于一个协调者 开放了 创建span的 api 入口
        // 内部的逻辑就是创建一个span对象 添加到活跃队列 并且启动span   如何这是发起的第二环 那么会复用之前的TracerContext (dubbo 会传递本地线程变量)
        AbstractSpan span = context.createExitSpan(operationName, remotePeer);
        // 这里将context信息注入到carrier中
        context.inject(carrier);
        return span;
    }

    public static AbstractSpan createExitSpan(String operationName, String remotePeer) {
        operationName = StringUtil.cut(operationName, OPERATION_NAME_THRESHOLD);
        AbstractTracerContext context = getOrCreate(operationName, false);
        return context.createExitSpan(operationName, remotePeer);
    }

    /**
     * 获取当前上下文信息 并注入到carrier 中
     * @param carrier
     */
    public static void inject(ContextCarrier carrier) {
        get().inject(carrier);
    }

    public static void extract(ContextCarrier carrier) {
        if (carrier == null) {
            throw new IllegalArgumentException("ContextCarrier can't be null.");
        }
        if (carrier.isValid()) {
            get().extract(carrier);
        }
    }

    public static ContextSnapshot capture() {
        return get().capture();
    }

    public static void continued(ContextSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("ContextSnapshot can't be null.");
        }
        if (snapshot.isValid() && !snapshot.isFromCurrent()) {
            get().continued(snapshot);
        }
    }

    public static AbstractTracerContext awaitFinishAsync(AbstractSpan span) {
        final AbstractTracerContext context = get();
        // 获取当前活跃的span 阻塞等待异步调用结束
        AbstractSpan activeSpan = context.activeSpan();
        if (span != activeSpan) {
            throw new RuntimeException("Span is not the active in current context.");
        }
        return context.awaitFinishAsync();
    }

    /**
     * If not sure has the active span, use this method, will be cause NPE when has no active span, use
     * ContextManager::isActive method to determine whether there has the active span.
     */
    public static AbstractSpan activeSpan() {
        return get().activeSpan();
    }

    /**
     * Recommend use ContextManager::stopSpan(AbstractSpan span), because in that way, the TracingContext core could
     * verify this span is the active one, in order to avoid stop unexpected span. If the current span is hard to get or
     * only could get by low-performance way, this stop way is still acceptable.
     */
    public static void stopSpan() {
        // 获取当前线程绑定的context
        final AbstractTracerContext context = get();
        // 按照栈的顺序停止span
        stopSpan(context.activeSpan(), context);
    }

    public static void stopSpan(AbstractSpan span) {
        stopSpan(span, get());
    }

    /**
     * 终止本次调用
     * @param span
     * @param context
     */
    private static void stopSpan(AbstractSpan span, final AbstractTracerContext context) {
        if (context.stopSpan(span)) {
            // 如果所有span 都被关闭 从 threadLocal 上移除该Context  比如 从A 节点发起一个服务调用 一旦返回结果 本次调用链已经结束  而一个业务方法如果内部包含多个 dubbo调用 会被看作时多个链路
            CONTEXT.remove();
            RUNTIME_CONTEXT.remove();
        }
    }

    @Override
    public void prepare() {

    }

    @Override
    public void boot() {
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {

    }

    public static boolean isActive() {
        return get() != null;
    }

    /**
     * 返回绑定在当前线程的RuntimeContext 如果没有则进行创建
     * @return
     */
    public static RuntimeContext getRuntimeContext() {
        RuntimeContext runtimeContext = RUNTIME_CONTEXT.get();
        if (runtimeContext == null) {
            runtimeContext = new RuntimeContext(RUNTIME_CONTEXT);
            RUNTIME_CONTEXT.set(runtimeContext);
        }

        return runtimeContext;
    }
}
