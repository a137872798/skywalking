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

package org.apache.skywalking.apm.plugin.dubbo;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;

import java.lang.reflect.Method;

import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;

/**
 * {@link DubboInterceptor} define how to enhance class {@link com.alibaba.dubbo.monitor.support.MonitorFilter#invoke(Invoker,
 * Invocation)}. the trace context transport to the provider side by {@link RpcContext#attachments}.but all the version
 * of dubbo framework below 2.8.3 don't support {@link RpcContext#attachments}, we support another way to support it.
 * 该对象拦截的是  alibaba.dubbo  还有同名对象拦截的是  apache.dubbo
 */
public class DubboInterceptor implements InstanceMethodsAroundInterceptor {
    /**
     * <h2>Consumer:</h2> The serialized trace context data will
     * inject to the {@link RpcContext#attachments} for transport to provider side.
     * <p>
     * <h2>Provider:</h2> The serialized trace context data will extract from
     * {@link RpcContext#attachments}. current trace segment will ref if the serialize context data is not null.
     * 这个拦截器 会拦截所有发往外部的请求  并且他们共用一个 cotnext
     */
    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
                             MethodInterceptResult result) throws Throwable {
        // invoker invocation 对象包含了本次调用的信息
        Invoker invoker = (Invoker) allArguments[0];
        Invocation invocation = (Invocation) allArguments[1];
        RpcContext rpcContext = RpcContext.getContext();
        boolean isConsumer = rpcContext.isConsumerSide();
        // 获取本次请求的目标地址
        URL requestURL = invoker.getUrl();

        AbstractSpan span;

        final String host = requestURL.getHost();
        final int port = requestURL.getPort();

        // 代表是消费者端发起的调用     链路追踪是一定会完成的  至于是否会生成profile信息 需要看本次调用链路起点与 当前激活的 profileTask 是否一致
        if (isConsumer) {
            // 每次发起一个跨进程的请求时 就创建一个存储数据的 ContextCarrier 对象  这个对象存放的是 本次调用相关的span 信息
            // 某个节点在一次链路中 可能又作为提供者 又作为消费者 (A调用B服务 B服务在逻辑处理内 又调用了A 服务 (此时认为B服务还没有完成) A完成后回到B 之后B 才将结果返回A  这时才认为 整个调用链结束)
            // TracingContext 本身没有做 跨进程处理 也就是绑定在一端上 而没有相互打通  在seats 中 ThreadLocal 在不同进程间进行了传递
            final ContextCarrier contextCarrier = new ContextCarrier();
            // 这里生成span 对象 并将信息填充到 carrier 中  在整个链路调用的第一环 会创建 TracerContext 对象
            //
            span = ContextManager.createExitSpan(generateOperationName(requestURL, invocation), contextCarrier, host + ":" + port);
            //invocation.getAttachments().put("contextData", contextDataStr);
            //@see https://github.com/alibaba/dubbo/blob/dubbo-2.5.3/dubbo-rpc/dubbo-rpc-api/src/main/java/com/alibaba/dubbo/rpc/RpcInvocation.java#L154-L161
            // 将carrier 内部的信息格式化
            CarrierItem next = contextCarrier.items();
            while (next.hasNext()) {
                next = next.next();
                // 设置到 rpcContext 对象内 该对象会在传输过程中被携带 并可以被对段的拦截器处理
                rpcContext.getAttachments().put(next.getHeadKey(), next.getHeadValue());
                if (invocation.getAttachments().containsKey(next.getHeadKey())) {
                    invocation.getAttachments().remove(next.getHeadKey());
                }
            }
            // 作为消费者端 触发相关方法前
        } else {
            ContextCarrier contextCarrier = new ContextCarrier();
            // 这里会读取 消费者端设置的数据
            CarrierItem next = contextCarrier.items();
            while (next.hasNext()) {
                next = next.next();
                // 这里的数据就是  (rpcContext.getAttachments().put(next.getHeadKey(), next.getHeadValue())) 设置的
                // 这里调用 setHeadValue 实际上会用内部的数据填充 carrier  (间接调用  carrier.deserialize(headValue, ContextCarrier.HeaderVersion.v2))
                next.setHeadValue(rpcContext.getAttachment(next.getHeadKey()));
            }

            // 首先 消费者端 发出一个exitSpan 之后在 segment上添加一个span 对象  之后到达 提供者 此时提供者端生成一个  entrySpan
            span = ContextManager.createEntrySpan(generateOperationName(requestURL, invocation), contextCarrier);
        }

        // 无论生产者 还是消费者 创建完span 后都会进入这里

        // 为span 追加标签
        Tags.URL.set(span, generateRequestURL(requestURL, invocation));
        // 设置本次创建 span 的组件信息
        span.setComponent(ComponentsDefine.DUBBO);
        // 设置本次跨进程的方式
        SpanLayer.asRPCFramework(span);
    }

    /**
     * 当方法执行完后  比如从 consumer 发起一个 exit动作 会创建一个 exitSpan 之后对端生成结果并返回时 触发该方法
     * 或者 provider 接收到一个请求 创建一个 entrySpan 之后 返回结果给consumer之前 也会经过这里
     *
     * @param objInst
     * @param method
     * @param allArguments
     * @param argumentsTypes
     * @param ret
     * @return
     * @throws Throwable
     */
    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
                              Object ret) throws Throwable {
        // 结果的包装对象  包含attachment  value exception 等信息
        Result result = (Result) ret;
        // 本次结果生成了异常
        if (result != null && result.getException() != null) {
            dealException(result.getException());
        }

        // 代表本次 span 执行完成    每次从一个点发出调用后会生成span 对象 如果那个对象又发起一次调用就嵌套了一层span 按照栈的顺序 先结束 内部的span 在最终回到起点时 stop最开始创建的span
        // 推测这里是 终止 活跃队列尾部的span
        ContextManager.stopSpan();
        return ret;
    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
                                      Class<?>[] argumentsTypes, Throwable t) {
        dealException(t);
    }

    /**
     * Log the throwable, which occurs in Dubbo RPC service.
     */
    private void dealException(Throwable throwable) {
        // 获取当前活跃的 span 并设置异常信息
        AbstractSpan span = ContextManager.activeSpan();
        // 就是设置 span 内部的一个标识
        span.errorOccurred();
        // 填充内部的log 信息
        span.log(throwable);
    }

    /**
     * Format operation name. e.g. org.apache.skywalking.apm.plugin.test.Test.test(String)
     *
     * @return operation name.
     */
    private String generateOperationName(URL requestURL, Invocation invocation) {
        StringBuilder operationName = new StringBuilder();
        operationName.append(requestURL.getPath());
        operationName.append("." + invocation.getMethodName() + "(");
        for (Class<?> classes : invocation.getParameterTypes()) {
            operationName.append(classes.getSimpleName() + ",");
        }

        if (invocation.getParameterTypes().length > 0) {
            operationName.delete(operationName.length() - 1, operationName.length());
        }

        operationName.append(")");

        return operationName.toString();
    }

    /**
     * Format request url. e.g. dubbo://127.0.0.1:20880/org.apache.skywalking.apm.plugin.test.Test.test(String).
     *
     * @return request url.
     */
    private String generateRequestURL(URL url, Invocation invocation) {
        StringBuilder requestURL = new StringBuilder();
        requestURL.append(url.getProtocol() + "://");
        requestURL.append(url.getHost());
        requestURL.append(":" + url.getPort() + "/");
        requestURL.append(generateOperationName(url, invocation));
        return requestURL.toString();
    }
}
