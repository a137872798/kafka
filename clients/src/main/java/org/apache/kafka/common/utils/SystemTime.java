/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;

import java.util.function.Supplier;

/**
 * A time implementation that uses the system clock and sleep call. Use `Time.SYSTEM` instead of creating an instance
 * of this class.
 * time对象的默认实现 都是通过调用系统函数 并且没有缓存时间
 */
public class SystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        Utils.sleep(ms);
    }

    /**
     * 等待deadlineMs时间后检测条件是否符合 当超过deadlineMs时 抛出异常
     * @param obj The object that will be waited with {@link Object#wait()}. Note that it is the responsibility
     *      of the caller to call notify on this object when the condition is satisfied.
     * @param condition The condition we are awaiting
     * @param deadlineMs The deadline timestamp at which to raise a timeout error
     *
     * @throws InterruptedException
     */
    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
        synchronized (obj) {
            while (true) {
                if (condition.get())
                    return;

                long currentTimeMs = milliseconds();
                if (currentTimeMs >= deadlineMs)
                    throw new TimeoutException("Condition not satisfied before deadline");

                obj.wait(deadlineMs - currentTimeMs);
            }
        }
    }

}
