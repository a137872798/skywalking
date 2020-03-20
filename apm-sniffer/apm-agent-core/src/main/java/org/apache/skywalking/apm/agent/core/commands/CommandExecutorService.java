/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.skywalking.apm.agent.core.commands;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.commands.executor.NoopCommandExecutor;
import org.apache.skywalking.apm.agent.core.commands.executor.ProfileTaskCommandExecutor;
import org.apache.skywalking.apm.agent.core.commands.executor.ServiceResetCommandExecutor;
import org.apache.skywalking.apm.network.trace.component.command.BaseCommand;
import org.apache.skywalking.apm.network.trace.component.command.ProfileTaskCommand;
import org.apache.skywalking.apm.network.trace.component.command.ServiceResetCommand;

import java.util.HashMap;
import java.util.Map;

/**
 * Command executor service, acts like a routing executor that controls all commands' execution, is responsible for
 * managing all the mappings between commands and their executors, one can simply invoke {@link #execute(BaseCommand)}
 * and it will routes the command to corresponding executor.
 * <p>
 * Registering command executor for new command in {@link #commandExecutorMap} is required to support new command.
 * 当skywalking 探针 对类进行增加后 会启动所有 BootService
 * 该对象会维护命令以及其 处理器的映射关系 当接收到请求后通过该对象去处理
 */
@DefaultImplementor
public class CommandExecutorService implements BootService, CommandExecutor {
    private Map<String, CommandExecutor> commandExecutorMap;

    /**
     * 前置工作中插入2个 固定的命令
     * @throws Throwable
     */
    @Override
    public void prepare() throws Throwable {
        commandExecutorMap = new HashMap<String, CommandExecutor>();

        // Register all the supported commands with their executors here
        commandExecutorMap.put(ServiceResetCommand.NAME, new ServiceResetCommandExecutor());

        // Profile task executor
        commandExecutorMap.put(ProfileTaskCommand.NAME, new ProfileTaskCommandExecutor());
    }

    @Override
    public void boot() throws Throwable {

    }

    @Override
    public void onComplete() throws Throwable {

    }

    @Override
    public void shutdown() throws Throwable {

    }

    @Override
    public void execute(final BaseCommand command) throws CommandExecutionException {
        executorForCommand(command).execute(command);
    }

    /**
     * 从map中找到command 对应的executor
     * @param command
     * @return
     */
    private CommandExecutor executorForCommand(final BaseCommand command) {
        final CommandExecutor executor = commandExecutorMap.get(command.getCommand());
        if (executor != null) {
            return executor;
        }
        return NoopCommandExecutor.INSTANCE;
    }
}
