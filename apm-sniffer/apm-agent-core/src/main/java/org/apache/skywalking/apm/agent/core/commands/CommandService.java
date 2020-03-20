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
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.network.common.Command;
import org.apache.skywalking.apm.network.common.Commands;
import org.apache.skywalking.apm.network.trace.component.command.BaseCommand;
import org.apache.skywalking.apm.network.trace.component.command.CommandDeserializer;
import org.apache.skywalking.apm.network.trace.component.command.UnsupportedCommandException;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 该对象应该是用于将 command 发送到服务器
 */
@DefaultImplementor
public class CommandService implements BootService, Runnable {

    private static final ILog LOGGER = LogManager.getLogger(CommandService.class);

    private volatile boolean isRunning = true;
    /**
     * 使用单线程 线程池来发送command
     */
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    /**
     * 存放任务的阻塞队列
     */
    private LinkedBlockingQueue<BaseCommand> commands = new LinkedBlockingQueue<BaseCommand>(64);
    /**
     * 缓存最近发送的命令  serialNum 应该是类似于 reqId的东西
     */
    private CommandSerialNumberCache serialNumberCache = new CommandSerialNumberCache();

    @Override
    public void prepare() throws Throwable {
    }

    @Override
    public void boot() throws Throwable {
        executorService.submit(new RunnableWithExceptionProtection(this,
                // 当遇到异常时选择打印日志
                new RunnableWithExceptionProtection.CallbackWhenException() {
            @Override
            public void handle(final Throwable t) {
                LOGGER.error(t, "CommandService failed to execute commands");
            }
        }));
    }


    @Override
    public void run() {
        // CommandExecutorService 可以根据command 找到匹配的处理器
        final CommandExecutorService commandExecutorService = ServiceManager.INSTANCE.findService(CommandExecutorService.class);

        while (isRunning) {
            try {
                // 从阻塞队列中取出command
                BaseCommand command = commands.take();

                // 代表command 对应的 reqId 已经被处理过了 那么就忽略本次数据
                if (isCommandExecuted(command)) {
                    continue;
                }

                // 开始处理command 并且添加到容器中
                commandExecutorService.execute(command);
                serialNumberCache.add(command.getSerialNumber());
            } catch (InterruptedException e) {
                LOGGER.error(e, "Failed to take commands.");
            } catch (CommandExecutionException e) {
                LOGGER.error(e, "Failed to execute command[{}].", e.command().getCommand());
            } catch (Throwable e) {
                LOGGER.error(e, "There is unexpected exception");
            }
        }
    }

    /**
     * 本处理过的command 会保存到 一个缓存中 如果已经保存过就代表已经处理过了 它应该是避免重复消费数据  TCP 的重发数据吗
     * @param command
     * @return
     */
    private boolean isCommandExecuted(BaseCommand command) {
        return serialNumberCache.contain(command.getSerialNumber());
    }

    @Override
    public void onComplete() throws Throwable {

    }

    /**
     * 当终止时 关闭线程池
     * @throws Throwable
     */
    @Override
    public void shutdown() throws Throwable {
        isRunning = false;
        // 将数据转移到一个空的list  为啥不用clear()
        commands.drainTo(new ArrayList<BaseCommand>());
        executorService.shutdown();
    }

    /**
     * 当接收到一个命令时 就是加入到阻塞队列  命令会批量发送
     * @param commands
     */
    public void receiveCommand(Commands commands) {
        // 遍历内部的command
        for (Command command : commands.getCommandsList()) {
            try {
                BaseCommand baseCommand = CommandDeserializer.deserialize(command);

                if (isCommandExecuted(baseCommand)) {
                    LOGGER.warn("Command[{}] is executed, ignored", baseCommand.getCommand());
                    continue;
                }

                // 判断是否添加成功 当添加失败时 打印日志 同时抛弃数据
                boolean success = this.commands.offer(baseCommand);

                if (!success && LOGGER.isWarnEnable()) {
                    LOGGER.warn("Command[{}, {}] cannot add to command list. because the command list is full.", baseCommand
                        .getCommand(), baseCommand.getSerialNumber());
                }
            } catch (UnsupportedCommandException e) {
                if (LOGGER.isWarnEnable()) {
                    LOGGER.warn("Received unsupported command[{}].", e.getCommand().getCommand());
                }
            }
        }
    }
}
