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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.conf.RemoteDownstreamConfig;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractTracingSpan;
import org.apache.skywalking.apm.agent.core.context.trace.EntrySpan;
import org.apache.skywalking.apm.agent.core.context.trace.ExitSpan;
import org.apache.skywalking.apm.agent.core.context.trace.LocalSpan;
import org.apache.skywalking.apm.agent.core.context.trace.NoopExitSpan;
import org.apache.skywalking.apm.agent.core.context.trace.NoopSpan;
import org.apache.skywalking.apm.agent.core.context.trace.TraceSegment;
import org.apache.skywalking.apm.agent.core.context.trace.TraceSegmentRef;
import org.apache.skywalking.apm.agent.core.context.trace.WithPeerInfo;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryManager;
import org.apache.skywalking.apm.agent.core.dictionary.DictionaryUtil;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.agent.core.profile.ProfileTaskExecutionService;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;
import org.apache.skywalking.apm.util.StringUtil;

/**
 * The <code>TracingContext</code> represents a core tracing logic controller. It build the final {@link
 * TracingContext}, by the stack mechanism, which is similar with the codes work.
 * <p>
 * In opentracing concept, it means, all spans in a segment tracing context(thread) are CHILD_OF relationship, but no
 * FOLLOW_OF.
 * <p>
 * In skywalking core concept, FOLLOW_OF is an abstract concept when cross-process MQ or cross-thread async/batch tasks
 * happen, we used {@link TraceSegmentRef} for these scenarios. Check {@link TraceSegmentRef} which is from {@link
 * ContextCarrier} or {@link ContextSnapshot}.
 * 当跨进程时 会创建一个新的 tracingContext 对象
 */
public class TracingContext implements AbstractTracerContext {
    private static final ILog logger = LogManager.getLogger(TracingContext.class);

    /**
     * 最后一次警告时间
     */
    private long lastWarningTimestamp = 0;

    /**
     * @see ProfileTaskExecutionService
     * 该对象相当于是整个 profile体系的入口 一个TracingContext 会绑定一个 ThreadProfiler 定时读取线程堆栈信息 并生成对应的快照对象
     */
    private static ProfileTaskExecutionService PROFILE_TASK_EXECUTION_SERVICE;

    /**
     * @see SamplingService
     * 用于判断能否正常添加样本的任务
     */
    private static SamplingService SAMPLING_SERVICE;

    /**
     * The final {@link TraceSegment}, which includes all finished spans.
     * 一个段对象内部包含一组 span
     */
    private TraceSegment segment;

    /**
     * Active spans stored in a Stack, usually called 'ActiveSpanStack'. This {@link LinkedList} is the in-memory
     * storage-structure. <p> I use {@link LinkedList#removeLast()}, {@link LinkedList#addLast(Object)} and {@link
     * LinkedList#getLast()} instead of {@link #pop()}, {@link #push(AbstractSpan)}, {@link #peek()}
     * 链接到一组当前活跃的 span
     */
    private LinkedList<AbstractSpan> activeSpanStack = new LinkedList<>();

    /**
     * A counter for the next span.
     * 这个也是 spanId  也是计数器 代表一次跨进程调用涉及到的所有节点中 每个节点上出现了多少次 出入 (exit/entry)  默认情况下不能超过3000次
     */
    private int spanIdGenerator;

    /**
     * The counter indicates
     */
    @SuppressWarnings("unused") // updated by ASYNC_SPAN_COUNTER_UPDATER
    private volatile int asyncSpanCounter;
    private static final AtomicIntegerFieldUpdater<TracingContext> ASYNC_SPAN_COUNTER_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(TracingContext.class, "asyncSpanCounter");

    /**
     * 是否在异步模式下启动
     */
    private volatile boolean isRunningInAsyncMode;
    private volatile ReentrantLock asyncFinishLock;

    private volatile boolean running;

    private final long createTime;

    /**
     * profiling status  是否刚好有探测本次操作的任务 如果有的话 刚好将自身添加到 profileThread 的槽中
     */
    private volatile boolean profiling;

    /**
     * Initialize all fields with default value.
     * 描述本次操作的信息 比如 dubbo 调用了哪个服务提供者 调用了什么方法 使用了什么参数
     * 就是为了解耦才需要这个 context 对象  而 专门负责记录链路的对象就是 TraceSegment   context 相当于一个纽带
     */
    TracingContext(String firstOPName) {
        // 当整个链路上下文被初始化的时候 首先创建一个 段对象
        this.segment = new TraceSegment();
        this.spanIdGenerator = 0;
        // 默认情况下属于 非异步模式
        isRunningInAsyncMode = false;
        createTime = System.currentTimeMillis();
        running = true;

        if (SAMPLING_SERVICE == null) {
            SAMPLING_SERVICE = ServiceManager.INSTANCE.findService(SamplingService.class);
        }

        // profiling status
        if (PROFILE_TASK_EXECUTION_SERVICE == null) {
            PROFILE_TASK_EXECUTION_SERVICE = ServiceManager.INSTANCE.findService(ProfileTaskExecutionService.class);
        }

        // 添加针对本上下文进行的堆栈监控  必须先确保 service 当前要生成的profileTask 刚好是针对该operateName
        this.profiling = PROFILE_TASK_EXECUTION_SERVICE.addProfiling(this, segment.getTraceSegmentId(), firstOPName);
    }

    /**
     * Inject the context into the given carrier, only when the active span is an exit one.
     *
     * @param carrier to carry the context for crossing process.
     * @throws IllegalStateException if the active span isn't an exit one. Ref to {@link AbstractTracerContext#inject(ContextCarrier)}
     *                               将context内信息填充到 ContextCarrier 中
     *                               每次添加一个新的 span 时 内部信息也会发生变化
     */
    @Override
    public void inject(ContextCarrier carrier) {
        // 获取最后一个span对象
        AbstractSpan span = this.activeSpan();
        if (!span.isExit()) {
            throw new IllegalStateException("Inject can be done only in Exit Span");
        }

        // 这些信息将会转换 之后发送到 oap 需要关注这里设置了什么属性

        // 获取对端信息 比如这次调用了哪个服务提供者
        WithPeerInfo spanWithPeer = (WithPeerInfo) span;
        String peer = spanWithPeer.getPeer();
        int peerId = spanWithPeer.getPeerId();

        // 代表本次 span 是属于哪个 segment的
        carrier.setTraceSegmentId(this.segment.getTraceSegmentId());
        // 设置本次span
        carrier.setSpanId(span.getSpanId());

        // 设置当前服务实例 id    本节点启动时 会将自身注册到 oap上  当注册成功时 就会填充服务实例id 和服务id (这些信息都是从config中获得的)
        carrier.setParentServiceInstanceId(segment.getApplicationInstanceId());

        // 填充对端信息
        if (DictionaryUtil.isNull(peerId)) {
            carrier.setPeerHost(peer);
        } else {
            carrier.setPeerId(peerId);
        }

        // 获取活跃队列中第一个span
        AbstractSpan firstSpan = first();
        // 获取发起整个调用链的首个操作
        String firstSpanOperationName = firstSpan.getOperationName();

        // 获取该segment 关联的其他段
        List<TraceSegmentRef> refs = this.segment.getRefs();
        int operationId = DictionaryUtil.inexistence();
        String operationName = "";
        int entryApplicationInstanceId;

        // 如果当前上下文是 由其他segment 间接创建的  比如A 服务调用B 服务 那么B服务的segment 就是A服务间接创建的
        if (refs != null && refs.size() > 0) {

            TraceSegmentRef ref = refs.get(0);
            operationId = ref.getEntryEndpointId();
            operationName = ref.getEntryEndpointName();
            // 重置整个入口的 id
            entryApplicationInstanceId = ref.getEntryServiceInstanceId();
        } else {
            if (firstSpan.isEntry()) {
                /*
                 * Since 6.6.0, if first span is not entry span, then this is an internal segment(no RPC),
                 * rather than an endpoint.
                 */
                operationId = firstSpan.getOperationId();
                operationName = firstSpanOperationName;
            }
            entryApplicationInstanceId = this.segment.getApplicationInstanceId();

        }
        carrier.setEntryServiceInstanceId(entryApplicationInstanceId);

        if (operationId == DictionaryUtil.nullValue()) {
            if (!StringUtil.isEmpty(operationName)) {
                carrier.setEntryEndpointName(operationName);
            } else {
                /*
                 * Since 6.6.0, if first span is not entry span, then this is an internal segment(no RPC),
                 * rather than an endpoint.
                 */
            }
        } else {
            carrier.setEntryEndpointId(operationId);
        }

        int parentOperationId = firstSpan.getOperationId();
        if (parentOperationId == DictionaryUtil.nullValue()) {
            if (firstSpan.isEntry() && !StringUtil.isEmpty(firstSpanOperationName)) {
                carrier.setParentEndpointName(firstSpanOperationName);
            } else {
                /*
                 * Since 6.6.0, if first span is not entry span, then this is an internal segment(no RPC),
                 * rather than an endpoint.
                 */
                carrier.setParentEndpointId(DictionaryUtil.inexistence());
            }
        } else {
            carrier.setParentEndpointId(parentOperationId);
        }

        // 这里记录了所有segment的id  (当跨进程调用时 每次都会将 之前segment的id 追加到 segment.relatedGlobalTraces 中)
        carrier.setDistributedTraceIds(this.segment.getRelatedGlobalTraces());
    }

    /**
     * Extract the carrier to build the reference for the pre segment.
     *
     * @param carrier carried the context from a cross-process segment. Ref to {@link AbstractTracerContext#extract(ContextCarrier)}
     *                代表本链路上下文  依赖于某个carrier
     */
    @Override
    public void extract(ContextCarrier carrier) {
        // 抽取carrier 内部的数据并生成一个 ref
        TraceSegmentRef ref = new TraceSegmentRef(carrier);
        // 追加一个 ref 对象
        this.segment.ref(ref);
        // 记录整个链路第一环的 链路id
        this.segment.relatedGlobalTraces(carrier.getDistributedTraceId());
        // 获取当前最后一个span 并且设置ref  每个span 对象内部也有一个 ref列表
        AbstractSpan span = this.activeSpan();
        if (span instanceof EntrySpan) {
            span.ref(ref);
        }
    }

    /**
     * Capture the snapshot of current context.
     *
     * @return the snapshot of context for cross-thread propagation Ref to {@link AbstractTracerContext#capture()}
     * 获取当前内部信息 并生成一个快照对象
     */
    @Override
    public ContextSnapshot capture() {
        // 获取当前segment 依赖的所有 segment
        List<TraceSegmentRef> refs = this.segment.getRefs();
        // 通过当前信息生成一个快照对象
        ContextSnapshot snapshot = new ContextSnapshot(segment.getTraceSegmentId(), activeSpan().getSpanId(), segment.getRelatedGlobalTraces());
        int entryOperationId;
        String entryOperationName = "";
        int entryApplicationInstanceId;
        AbstractSpan firstSpan = first();
        String firstSpanOperationName = firstSpan.getOperationName();

        if (refs != null && refs.size() > 0) {
            TraceSegmentRef ref = refs.get(0);
            entryOperationId = ref.getEntryEndpointId();
            entryOperationName = ref.getEntryEndpointName();
            entryApplicationInstanceId = ref.getEntryServiceInstanceId();
        } else {
            if (firstSpan.isEntry()) {
                entryOperationId = firstSpan.getOperationId();
                entryOperationName = firstSpanOperationName;
            } else {
                /*
                 * Since 6.6.0, if first span is not entry span, then this is an internal segment(no RPC),
                 * rather than an endpoint.
                 */
                entryOperationId = DictionaryUtil.inexistence();
            }
            entryApplicationInstanceId = this.segment.getApplicationInstanceId();
        }
        snapshot.setEntryApplicationInstanceId(entryApplicationInstanceId);

        if (entryOperationId == DictionaryUtil.nullValue()) {
            if (!StringUtil.isEmpty(entryOperationName)) {
                snapshot.setEntryOperationName(entryOperationName);
            } else {
                /*
                 * Since 6.6.0, if first span is not entry span, then this is an internal segment(no RPC),
                 * rather than an endpoint.
                 */
            }
        } else {
            snapshot.setEntryOperationId(entryOperationId);
        }

        int parentOperationId = firstSpan.getOperationId();
        if (parentOperationId == DictionaryUtil.nullValue()) {
            if (firstSpan.isEntry() && !StringUtil.isEmpty(firstSpanOperationName)) {
                snapshot.setParentOperationName(firstSpanOperationName);
            } else {
                /*
                 * Since 6.6.0, if first span is not entry span, then this is an internal segment(no RPC),
                 * rather than an endpoint.
                 */
                snapshot.setParentOperationId(DictionaryUtil.inexistence());
            }
        } else {
            snapshot.setParentOperationId(parentOperationId);
        }
        return snapshot;
    }

    /**
     * Continue the context from the given snapshot of parent thread.
     *
     * @param snapshot from {@link #capture()} in the parent thread. Ref to {@link AbstractTracerContext#continued(ContextSnapshot)}
     *                 使用快照对象来生成 TraceSegmentRef 并填充 segment
     *                 感觉跟上面的方法是配对的
     */
    @Override
    public void continued(ContextSnapshot snapshot) {
        TraceSegmentRef segmentRef = new TraceSegmentRef(snapshot);
        this.segment.ref(segmentRef);
        this.activeSpan().ref(segmentRef);
        this.segment.relatedGlobalTraces(snapshot.getDistributedTraceId());
    }

    /**
     * @return the first global trace id.
     * 获取第一个关联的全局id
     */
    @Override
    public String getReadableGlobalTraceId() {
        return segment.getRelatedGlobalTraces().get(0).toString();
    }

    /**
     * Create an entry span
     *
     * @param operationName most likely a service name
     * @return span instance. Ref to {@link EntrySpan}
     * 当前上下文 根据操作名创建一个entrySpan 对象
     */
    @Override
    public AbstractSpan createEntrySpan(final String operationName) {
        // 如果该segment 对象生成的span 过多了  那么本次会生成一个空对象
        if (isLimitMechanismWorking()) {
            NoopSpan span = new NoopSpan();
            // 服务提供者端接收一个 entrySpan
            return push(span);
        }
        AbstractSpan entrySpan;
        TracingContext owner = this;
        // 先从队列中获取最后一个 span 对象
        final AbstractSpan parentSpan = peek();
        // 生成 该span 的 parentSpanId
        final int parentSpanId = parentSpan == null ? -1 : parentSpan.getSpanId();
        // 如果 上一个是 EntrySpan  而这次也是entry 同类型 所以不需要加入到队列中 只要重新启动 span 就可以了
        if (parentSpan != null && parentSpan.isEntry()) {
            entrySpan = (AbstractTracingSpan) DictionaryManager.findEndpointSection()
                    .findOnly(segment.getServiceId(), operationName)
                    // 传入本次的 operationName 如果找到了某个端点 那么为父对象设置 operationId
                    .doInCondition(parentSpan::setOperationId, () -> parentSpan
                            // 未找到时 设置操作名称
                            .setOperationName(operationName));
            // 重置parentSpan 的一些属性
            return entrySpan.start();
            // 好像同类型的span 是不需要连续入队列的 那么如果上一个span 是其他类型的 这里就必须创建一个entrySpan类型  同时如果存在parentSpanId 这里要沿用
        } else {
            entrySpan = (AbstractTracingSpan) DictionaryManager.findEndpointSection()
                    .findOnly(segment.getServiceId(), operationName)
                    // 新创建的span 对象 spanId 会递增
                    .doInCondition(operationId -> new EntrySpan(spanIdGenerator++, parentSpanId, operationId, owner), () -> {
                        return new EntrySpan(spanIdGenerator++, parentSpanId, operationName, owner);
                    });
            entrySpan.start();
            return push(entrySpan);
        }
    }

    /**
     * Create a local span
     *
     * @param operationName most likely a local method signature, or business name.
     * @return the span represents a local logic block. Ref to {@link LocalSpan}
     * 创建一个本地 span 上面的 entrySpan 应该代表着某种跨进程的操作 比如RPC
     */
    @Override
    public AbstractSpan createLocalSpan(final String operationName) {
        // 当超过最大限度时 同样传入一个span 对象 那么 使用一个空的span 会有什么影响呢
        if (isLimitMechanismWorking()) {
            NoopSpan span = new NoopSpan();
            return push(span);
        }
        AbstractSpan parentSpan = peek();
        final int parentSpanId = parentSpan == null ? -1 : parentSpan.getSpanId();
        /*
         * From v6.0.0-beta, local span doesn't do op name register.
         * All op name register is related to entry and exit spans only.
         * 本地 span 永远是重新创建 而不考虑复用
         */
        AbstractTracingSpan span = new LocalSpan(spanIdGenerator++, parentSpanId, operationName, this);
        span.start();
        return push(span);
    }

    /**
     * Create an exit span
     *
     * @param operationName most likely a service name of remote
     * @param remotePeer    the network id(ip:port, hostname:port or ip1:port1,ip2,port, etc.)   描述对端信息 比如 dubbo中的提供者信息
     * @return the span represent an exit point of this segment.
     * @see ExitSpan
     * 代表某个链路出现了一个 exit 的动作
     */
    @Override
    public AbstractSpan createExitSpan(final String operationName, final String remotePeer) {
        if (isLimitMechanismWorking()) {
            // 如果本次操作被限制 那么创建一个空的span 对象
            NoopExitSpan span = new NoopExitSpan(remotePeer);
            // 将span 设置到一个活跃队列中  (这里没有跟segment 发生互动)
            return push(span);
        }

        AbstractSpan exitSpan;
        // 返回上个活跃的 span  也就是从 活跃队列 中获取最后一个span 最为本span 的父对象
        AbstractSpan parentSpan = peek();
        TracingContext owner = this;
        // 如果父span 也是 exit 那么选择续用之前的span   也就是一个consumer 调用 某个producer 而该 producer 又依赖另一个 producer 选择复用这个span
        // 这里应该是模拟一种嵌套的情况 但是在 dubbo中是不会出现的
        if (parentSpan != null && parentSpan.isExit()) {
            exitSpan = parentSpan;
        } else {
            // 代表类型不同 创建一个新的span   或者该span 是链路中第一个span  默认id 是-1
            final int parentSpanId = parentSpan == null ? -1 : parentSpan.getSpanId();
            exitSpan = (AbstractSpan) DictionaryManager.findNetworkAddressSection()
                    // 找到远端地址 对应的应用信息  如果没有找到会在后台线程将 地址信息注册到 oap 并且返还 appId (就是在持久层作为唯一标识的id )
                    .find(remotePeer)
                    // 当找到时 使用 peerId 作为生成 exitSpan的参数   否则直接使用remotePeer作为span的参数
                    .doInCondition(peerId -> new ExitSpan(spanIdGenerator++, parentSpanId, operationName, peerId, owner), () -> {
                        return new ExitSpan(spanIdGenerator++, parentSpanId, operationName, remotePeer, owner);
                    });
            // 将span 设置到当前活跃队列中  （记录当前未结束的所有span）
            push(exitSpan);
        }
        // 代表某个span 已经被启动
        exitSpan.start();
        return exitSpan;
    }

    /**
     * @return the active span of current context, the top element of {@link #activeSpanStack}
     * 从队列中获取一个当前活跃的 span
     */
    @Override
    public AbstractSpan activeSpan() {
        AbstractSpan span = peek();
        if (span == null) {
            throw new IllegalStateException("No active span.");
        }
        return span;
    }

    /**
     * Stop the given span, if and only if this one is the top element of {@link #activeSpanStack}. Because the tracing
     * core must make sure the span must match in a stack module, like any program did.
     *
     * @param span to finish
     *             要结束某个span
     */
    @Override
    public boolean stopSpan(AbstractSpan span) {
        AbstractSpan lastSpan = peek();
        // 确保先进先出的顺序   sentinel 好像有类似的逻辑 里面好像也有个span 必须按照先进先出的顺序
        if (lastSpan == span) {
            if (lastSpan instanceof AbstractTracingSpan) {
                AbstractTracingSpan toFinishSpan = (AbstractTracingSpan) lastSpan;
                if (toFinishSpan.finish(segment)) {
                    // 成功时 从当前 activeSpan 中移除
                    pop();
                }
            } else {
                // 代表是 NoopSpan 或者是 LocalSpan 直接移除
                pop();
            }
        } else {
            throw new IllegalStateException("Stopping the unexpected span = " + span);
        }

        // 代表处理完成
        finish();

        // 返回是否清除了所有的span
        return activeSpanStack.isEmpty();
    }

    /**
     * 等待异步结束
     *
     * @return
     */
    @Override
    public AbstractTracerContext awaitFinishAsync() {
        if (!isRunningInAsyncMode) {
            synchronized (this) {
                // 初始化异步相关的信息
                if (!isRunningInAsyncMode) {
                    asyncFinishLock = new ReentrantLock();
                    ASYNC_SPAN_COUNTER_UPDATER.set(this, 0);
                    isRunningInAsyncMode = true;
                }
            }
        }
        // 增加异步计数器
        ASYNC_SPAN_COUNTER_UPDATER.incrementAndGet(this);
        return this;
    }

    /**
     * 终止异步操作
     *
     * @param span to be stopped.
     */
    @Override
    public void asyncStop(AsyncSpan span) {
        ASYNC_SPAN_COUNTER_UPDATER.decrementAndGet(this);
        finish();
    }

    /**
     * Re-check current trace need profiling, encase third part plugin change the operation name.
     *
     * @param span          current modify span
     * @param operationName change to operation name
     */
    public void profilingRecheck(AbstractSpan span, String operationName) {
        // only recheck first span
        if (span.getSpanId() != 0) {
            return;
        }

        // 就是生成本对象对应的 线程堆栈检测对象
        profiling = PROFILE_TASK_EXECUTION_SERVICE.profilingRecheck(this, segment.getTraceSegmentId(), operationName);
    }

    /**
     * Finish this context, and notify all {@link TracingContextListener}s, managed by {@link
     * TracingContext.ListenerManager} and {@link TracingContext.TracingThreadListenerManager}
     * 每次调用 stopSpan 会间接触发该方法
     */
    private void finish() {
        // 如果是在异步模式下 首先要上锁  负责该方法可能被并发访问 为了确保一致性和可见性就要上锁
        if (isRunningInAsyncMode) {
            asyncFinishLock.lock();
        }
        try {
            // 代表 本次的 stopSpan 刚好是发起第一次操作的地方  那么本次整个调用链结束
            boolean isFinishedInMainThread = activeSpanStack.isEmpty() && running;
            if (isFinishedInMainThread) {
                /*
                 * Notify after tracing finished in the main thread.
                 */
                TracingThreadListenerManager.notifyFinish(this);
            }

            // 代表在同步模式下 并且当前是 整个调用链结束
            if (isFinishedInMainThread && (!isRunningInAsyncMode || asyncSpanCounter == 0)) {
                // 终止整个段对象
                TraceSegment finishedSegment = segment.finish(isLimitMechanismWorking());
                /*
                 * Recheck the segment if the segment contains only one span.
                 * Because in the runtime, can't sure this segment is part of distributed trace.
                 *
                 * @see {@link #createSpan(String, long, boolean)}
                 * 如果该 segment 只创建了一个span 对象
                 */
                if (!segment.hasRef() && segment.isSingleSpanSegment()) {
                    // 能否申请到样本token  该算法实际上算是一个令牌桶
                    if (!SAMPLING_SERVICE.trySampling()) {
                        finishedSegment.setIgnore(true);
                    }
                }

                /*
                 * Check that the segment is created after the agent (re-)registered to backend,
                 * otherwise the segment may be created when the agent is still rebooting and should
                 * be ignored
                 * 如果该segment 创建的过早了  这之后发生了某种重启动作 那么 就不该生成样本
                 */
                if (segment.createTime() < RemoteDownstreamConfig.Agent.INSTANCE_REGISTERED_TIME) {
                    finishedSegment.setIgnore(true);
                }

                TracingContext.ListenerManager.notifyFinish(finishedSegment);

                running = false;
            }
        } finally {
            if (isRunningInAsyncMode) {
                asyncFinishLock.unlock();
            }
        }
    }

    /**
     * The <code>ListenerManager</code> represents an event notify for every registered listener, which are notified
     * when the <code>TracingContext</code> finished, and {@link #segment} is ready for further process.
     * 当某个链路结束时 触发所有监听器
     */
    public static class ListenerManager {
        private static List<TracingContextListener> LISTENERS = new LinkedList<>();

        /**
         * Add the given {@link TracingContextListener} to {@link #LISTENERS} list.
         *
         * @param listener the new listener.
         */
        public static synchronized void add(TracingContextListener listener) {
            LISTENERS.add(listener);
        }

        /**
         * Notify the {@link TracingContext.ListenerManager} about the given {@link TraceSegment} have finished. And
         * trigger {@link TracingContext.ListenerManager} to notify all {@link #LISTENERS} 's {@link
         * TracingContextListener#afterFinished(TraceSegment)}
         *
         * @param finishedSegment the segment that has finished
         */
        static void notifyFinish(TraceSegment finishedSegment) {
            for (TracingContextListener listener : LISTENERS) {
                listener.afterFinished(finishedSegment);
            }
        }

        /**
         * Clear the given {@link TracingContextListener}
         */
        public static synchronized void remove(TracingContextListener listener) {
            LISTENERS.remove(listener);
        }

    }

    /**
     * The <code>ListenerManager</code> represents an event notify for every registered listener, which are notified
     * 内部维护了一组监听器
     */
    public static class TracingThreadListenerManager {
        private static List<TracingThreadListener> LISTENERS = new LinkedList<>();

        public static synchronized void add(TracingThreadListener listener) {
            LISTENERS.add(listener);
        }

        /**
         * 通知监听器 某个 TraceingContext 的链路执行完了
         * @param finishedContext
         */
        static void notifyFinish(TracingContext finishedContext) {
            for (TracingThreadListener listener : LISTENERS) {
                listener.afterMainThreadFinish(finishedContext);
            }
        }

        public static synchronized void remove(TracingThreadListener listener) {
            LISTENERS.remove(listener);
        }
    }

    /**
     * @return the top element of 'ActiveSpanStack', and remove it.
     */
    private AbstractSpan pop() {
        return activeSpanStack.removeLast();
    }

    /**
     * Add a new Span at the top of 'ActiveSpanStack'
     *
     * @param span the {@code span} to push
     *             span 入队列
     */
    private AbstractSpan push(AbstractSpan span) {
        activeSpanStack.addLast(span);
        return span;
    }

    /**
     * @return the top element of 'ActiveSpanStack' only.
     */
    private AbstractSpan peek() {
        if (activeSpanStack.isEmpty()) {
            return null;
        }
        return activeSpanStack.getLast();
    }

    private AbstractSpan first() {
        return activeSpanStack.getFirst();
    }

    /**
     * 判断是否 启动限制机制
     * @return
     */
    private boolean isLimitMechanismWorking() {
        // 启动状态下 每个 segment 只能追踪3000 个span
        if (spanIdGenerator >= Config.Agent.SPAN_LIMIT_PER_SEGMENT) {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastWarningTimestamp > 30 * 1000) {
                logger.warn(new RuntimeException("Shadow tracing context. Thread dump"), "More than {} spans required to create", Config.Agent.SPAN_LIMIT_PER_SEGMENT);
                lastWarningTimestamp = currentTimeMillis;
            }
            return true;
        } else {
            return false;
        }
    }

    public long createTime() {
        return this.createTime;
    }

    public boolean isProfiling() {
        return this.profiling;
    }

}
