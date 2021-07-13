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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The future result of a record send
 * 针对单个record 管理获取发送结果 batch会管理thunk 而thunk内部包含该对象
 * producer调用send后 得到的就是这个对象
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {

    /**
     * 本对象负责阻塞一个batch对象
     */
    private final ProduceRequestResult result;
    private final long relativeOffset;
    private final long createTimestamp;
    private final Long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final Time time;

    /**
     * 可以将多个等待发送结果的对象组合成链式结构  在调用get时 只有这组消息都发送成功时 才会从阻塞状态解除
     */
    private volatile FutureRecordMetadata nextRecordMetadata = null;

    /**
     *
     * @param result
     * @param relativeOffset 这个偏移量是代表本record对应batch中的第几个
     * @param createTimestamp
     * @param checksum
     * @param serializedKeySize
     * @param serializedValueSize
     * @param time
     */
    public FutureRecordMetadata(ProduceRequestResult result, long relativeOffset, long createTimestamp,
                                Long checksum, int serializedKeySize, int serializedValueSize, Time time) {
        this.result = result;
        this.relativeOffset = relativeOffset;
        this.createTimestamp = createTimestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.time = time;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    /**
     * 可以看到针对某个record的get 会由同一个result对象进行阻塞  而result与batch是一一对应的
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        this.result.await();
        // 如果本对象已经构建成链式结构后 一旦本对象从阻塞状态唤醒 就会立即调用下一个对象的get
        if (nextRecordMetadata != null)
            return nextRecordMetadata.get();
        return valueOrError();
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        // Handle overflow.
        long now = time.milliseconds();
        long timeoutMillis = unit.toMillis(timeout);
        long deadline = Long.MAX_VALUE - timeoutMillis < now ? Long.MAX_VALUE : now + timeoutMillis;
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + timeoutMillis + " ms.");
        if (nextRecordMetadata != null)
            return nextRecordMetadata.get(deadline - time.milliseconds(), TimeUnit.MILLISECONDS);
        return valueOrError();
    }

    /**
     * This method is used when we have to split a large batch in smaller ones. A chained metadata will allow the
     * future that has already returned to the users to wait on the newly created split batches even after the
     * old big batch has been deemed as done.
     */
    void chain(FutureRecordMetadata futureRecordMetadata) {
        if (nextRecordMetadata == null)
            nextRecordMetadata = futureRecordMetadata;
        else
            nextRecordMetadata.chain(futureRecordMetadata);
    }

    RecordMetadata valueOrError() throws ExecutionException {
        if (this.result.error() != null)
            throw new ExecutionException(this.result.error());
        else
            return value();
    }

    Long checksumOrNull() {
        return this.checksum;
    }

    /**
     * 将本消息的相关信息返回
     * @return
     */
    RecordMetadata value() {
        // 如果是链式结构 则返回链尾的记录
        if (nextRecordMetadata != null)
            return nextRecordMetadata.value();
        return new RecordMetadata(result.topicPartition(), this.result.baseOffset(), this.relativeOffset,
                                  timestamp(), this.checksum, this.serializedKeySize, this.serializedValueSize);
    }

    private long timestamp() {
        return result.hasLogAppendTime() ? result.logAppendTime() : createTimestamp;
    }

    @Override
    public boolean isDone() {
        if (nextRecordMetadata != null)
            return nextRecordMetadata.isDone();
        return this.result.completed();
    }

}
