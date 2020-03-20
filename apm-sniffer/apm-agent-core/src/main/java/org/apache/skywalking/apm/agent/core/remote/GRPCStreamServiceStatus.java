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

package org.apache.skywalking.apm.agent.core.remote;

import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;

/**
 * 基于流式调用的先不看 就是模仿reactor的api 来调用
 */
public class GRPCStreamServiceStatus {
    private static final ILog logger = LogManager.getLogger(GRPCStreamServiceStatus.class);
    private volatile boolean status;

    public GRPCStreamServiceStatus(boolean status) {
        this.status = status;
    }

    public boolean isStatus() {
        return status;
    }

    public void finished() {
        this.status = true;
    }

    /**
     * Wait until success status reported.
     */
    public void wait4Finish() {
        long recheckCycle = 5;
        long hasWaited = 0L;
        long maxCycle = 30 * 1000L; // 30 seconds max.
        while (!status) {
            try2Sleep(recheckCycle);
            hasWaited += recheckCycle;

            if (recheckCycle >= maxCycle) {
                logger.warn("Collector traceSegment service doesn't response in {} seconds.", hasWaited / 1000);
            } else {
                recheckCycle = Math.min(recheckCycle * 2, maxCycle);
            }
        }
    }

    /**
     * Try to sleep, and ignore the {@link InterruptedException}
     *
     * @param millis the length of time to sleep in milliseconds
     */
    private void try2Sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {

        }
    }
}
