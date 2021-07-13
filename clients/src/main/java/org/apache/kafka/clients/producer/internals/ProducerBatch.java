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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 * 在accumulator中针对每个tp都有一个 batch列表 每当待发送消息满足一个batch时就会触发发送逻辑
 */
public final class ProducerBatch {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    private enum FinalState { ABORTED, FAILED, SUCCEEDED }

    final long createdMs;
    final TopicPartition topicPartition;
    /**
     * 通过该对象阻塞对内部所有record响应结果的获取
     */
    final ProduceRequestResult produceFuture;

    private final List<Thunk> thunks = new ArrayList<>();
    private final MemoryRecordsBuilder recordsBuilder;
    private final AtomicInteger attempts = new AtomicInteger(0);
    /**
     * 本对象是否是由某个batch分裂产生的
     */
    private final boolean isSplitBatch;
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);

    int recordCount;
    int maxRecordSize;
    /**
     * 最近一次尝试发送的时间 默认就是创建时间
     */
    private long lastAttemptMs;
    private long lastAppendTime;
    private long drainedMs;
    private boolean retry;
    private boolean reopened;

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        this(tp, recordsBuilder, createdMs, false);
    }

    /**
     * 初始化batch对象
     * @param tp 本batch内所有的record都会发往该 tp
     * @param recordsBuilder 使用该对象来存储数据
     * @param createdMs 本对象的创建时间
     * @param isSplitBatch
     */
    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
        this.createdMs = createdMs;
        this.lastAttemptMs = createdMs;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.retry = false;
        this.isSplitBatch = isSplitBatch;
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                                                                                recordsBuilder.compressionType());
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     * 尝试追加一个消息
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 实际上存储数据的是builder对象 这里是检测builder是否有足够的空间  空间不足返回null
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            // 尝试插入数据 并且返回一个future对象 当该对象被唤醒时代表对应的record收到了结果  新版本不会返回校验和
            Long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
            // 更新本batch中最大的一个record
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            this.lastAppendTime = now;
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length,
                                                                   Time.SYSTEM);
            // we have to keep every future returned to the users in case the batch needs to be
            // split to several new batches and resent.
            // 将用户回调与 阻塞等待响应结果的future封装在一起
            thunks.add(new Thunk(callback, future));
            this.recordCount++;
            return future;
        }
    }

    /**
     * This method is only used by {@link #split(int)} when splitting a large batch to smaller ones.
     * @return true if the record has been successfully appended, false otherwise.
     * 在一个分裂batch的场景下调用该方法
     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return false;
        } else {
            // No need to get the CRC.
            this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, thunk.future.checksumOrNull(),
                                                                   key == null ? -1 : key.remaining(),
                                                                   value == null ? -1 : value.remaining(),
                                                                   Time.SYSTEM);
            // Chain the future to the original thunk.
            // 这里使用的是未拆分前的thunk对象 会保留callback 然后由于发送成功与否被绑定到了新的future上了 (新的batch对象是否发送成功取决于新的produceFuture)
            // 同时新的batch中存储的thunk还是旧的 通过chain来关联到新的thunk上
            thunk.future.chain(future);
            this.thunks.add(thunk);
            this.recordCount++;
            return true;
        }
    }

    /**
     * Abort the batch and complete the future and callbacks.
     *
     * @param exception The exception to use to complete the future and awaiting callbacks.
     */
    public void abort(RuntimeException exception) {
        if (!finalState.compareAndSet(null, FinalState.ABORTED))
            throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());

        log.trace("Aborting batch for partition {}", topicPartition, exception);
        completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, exception);
    }

    /**
     * Return `true` if {@link #done(long, long, RuntimeException)} has been invoked at least once, `false` otherwise.
     */
    public boolean isDone() {
        return finalState() != null;
    }

    /**
     * Finalize the state of a batch. Final state, once set, is immutable. This function may be called
     * once or twice on a batch. It may be called twice if
     * 1. An inflight batch expires before a response from the broker is received. The batch's final
     * state is set to FAILED. But it could succeed on the broker and second time around batch.done() may
     * try to set SUCCEEDED final state.
     * 2. If a transaction abortion happens or if the producer is closed forcefully, the final state is
     * ABORTED but again it could succeed if broker responds with a success.
     *
     * Attempted transitions from [FAILED | ABORTED] --> SUCCEEDED are logged.
     * Attempted transitions from one failure state to the same or a different failed state are ignored.
     * Attempted transitions from SUCCEEDED to the same or a failed state throw an exception.
     *
     * @param baseOffset The base offset of the messages assigned by the server  记录本消息在server上的偏移量
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param exception The exception that occurred (or null if the request was successful)
     * @return true if the batch was completed successfully and false if the batch was previously aborted
     * 当某个batch的发送产生结果时触发 可能是发送成功 也可能是发送失败
     */
    public boolean done(long baseOffset, long logAppendTime, RuntimeException exception) {
        final FinalState tryFinalState = (exception == null) ? FinalState.SUCCEEDED : FinalState.FAILED;

        if (tryFinalState == FinalState.SUCCEEDED) {
            log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
        } else {
            log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, exception);
        }

        // 设置结果
        if (this.finalState.compareAndSet(null, tryFinalState)) {
            // 根据结果来触发回调
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, exception);
            return true;
        }

        // TODO 忽略并发设置的情况
        if (this.finalState.get() != FinalState.SUCCEEDED) {
            if (tryFinalState == FinalState.SUCCEEDED) {
                // Log if a previously unsuccessful batch succeeded later on.
                log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
                    tryFinalState, topicPartition, baseOffset, this.finalState.get());
            } else {
                // FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
                log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
                    this.finalState.get(), tryFinalState, topicPartition, baseOffset);
            }
        } else {
            // A SUCCESSFUL batch must not attempt another state change.
            throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
        }
        return false;
    }

    /**
     * 根据结果来唤醒阻塞的线程 以及触发回调 回调被封装在thunk中
     * @param baseOffset
     * @param logAppendTime
     * @param exception
     */
    private void completeFutureAndFireCallbacks(long baseOffset, long logAppendTime, RuntimeException exception) {
        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        produceFuture.set(baseOffset, logAppendTime, exception);

        // execute callbacks
        // 开始触发所有回调
        for (Thunk thunk : thunks) {
            try {
                if (exception == null) {
                    // 拿到之前发送消息的相关信息
                    RecordMetadata metadata = thunk.future.value();
                    // 触发回调对象
                    if (thunk.callback != null)
                        thunk.callback.onCompletion(metadata, null);
                } else {
                    if (thunk.callback != null)
                        thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        // 唤醒阻塞线程
        produceFuture.done();
    }

    /**
     * 将本对象按照splitBatchSize进行拆分
     * @param splitBatchSize
     * @return
     */
    public Deque<ProducerBatch> split(int splitBatchSize) {
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        // 这里存储了本次要发送的消息
        MemoryRecords memoryRecords = recordsBuilder.build();

        // 该对象可以按照kafka的batch消息头拆解出多个batch
        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
        if (!recordBatchIter.hasNext())
            throw new IllegalStateException("Cannot split an empty producer batch.");

        // 应当只能抽取出一个batch
        RecordBatch recordBatch = recordBatchIter.next();
        if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed())
            throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
                    "with version v0 and v1");

        if (recordBatchIter.hasNext())
            throw new IllegalArgumentException("A producer batch should only have one record batch.");

        // 这里对应的是每个消息
        Iterator<Thunk> thunkIter = thunks.iterator();
        // We always allocate batch size because we are already splitting a big batch.
        // And we also Retain the create time of the original batch.
        ProducerBatch batch = null;

        // 遍历之前插入的每条消息
        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            Thunk thunk = thunkIter.next();
            // 按照分裂后的大小生成batch
            if (batch == null)
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);

            // A newly created batch can always host the first message.
            // 代表该batch已经被填满 需要创建一个新的batch
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                batches.add(batch);
                batch.closeForRecordAppends();
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }

        // Close the last batch and add it to the batch list after split.
        if (batch != null) {
            batches.add(batch);
            batch.closeForRecordAppends();
        }

        // 原对象可以被唤醒了 然后线程就会自动阻塞到新的thunk上
        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, new RecordBatchTooLargeException());
        produceFuture.done();

        // TODO 普通消息baseSequence为-1
        if (hasSequence()) {
            int sequence = baseSequence();
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
            for (ProducerBatch newBatch : batches) {
                newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
                sequence += newBatch.recordCount;
            }
        }
        return batches;
    }

    /**
     * 重新分配一个batch的大小 并且将第一条记录存储到batch中
     * @param record
     * @param batchSize
     * @return
     */
    private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
        int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                recordsBuilder.compressionType(), record.key(), record.value(), record.headers()), batchSize);
        ByteBuffer buffer = ByteBuffer.allocate(initialSize);

        // Note that we intentionally do not set producer state (producerId, epoch, sequence, and isTransactional)
        // for the newly created batch. This will be set when the batch is dequeued for sending (which is consistent
        // with how normal batches are handled).
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compressionType(),
                TimestampType.CREATE_TIME, 0L);
        return new ProducerBatch(topicPartition, builder, this.createdMs, true);
    }

    public boolean isCompressed() {
        return recordsBuilder.compressionType() != CompressionType.NONE;
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     * 同时维护了 消息结果回调 和描述消息的元数据对象
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
        return deliveryTimeoutMs <= now - this.createdMs;
    }

    public FinalState finalState() {
        return this.finalState.get();
    }

    int attempts() {
        return attempts.get();
    }

    /**
     * 当batch发送失败时 会重新进入deque 同时更新最后一次尝试发送的时间
     * @param now
     */
    void reenqueued(long now) {
        attempts.getAndIncrement();
        lastAttemptMs = Math.max(lastAppendTime, now);
        lastAppendTime = Math.max(lastAppendTime, now);
        retry = true;
    }

    long queueTimeMs() {
        return drainedMs - createdMs;
    }

    long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    /**
     * 代表内部数据已经被取出
     * @param nowMs
     */
    void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                this.baseSequence(), this.topicPartition, baseSequence);
        reopened = true;
        recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     * 将内部的输出流标记成关闭状态 因为此时数据已经写满了
     */
    public void closeForRecordAppends() {
        recordsBuilder.closeForRecordAppends();
    }

    /**
     * 此时本对象即将发送 可以关闭内部的builder对象了
     */
    public void close() {
        recordsBuilder.close();
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                                                       recordsBuilder.compressionType(),
                                                       (float) recordsBuilder.compressionRatio());
        }
        reopened = false;
    }

    /**
     * Abort the record builder and reset the state of the underlying buffer. This is used prior to aborting
     * the batch with {@link #abort(RuntimeException)} and ensures that no record previously appended can be
     * read. This is used in scenarios where we want to ensure a batch ultimately gets aborted, but in which
     * it is not safe to invoke the completion callbacks (e.g. because we are holding a lock,
     * {@link RecordAccumulator#abortBatches()}).
     */
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }

    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    public byte magic() {
        return recordsBuilder.magic();
    }

    public long producerId() {
        return recordsBuilder.producerId();
    }

    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }

    public int baseSequence() {
        return recordsBuilder.baseSequence();
    }

    public int lastSequence() {
        return recordsBuilder.baseSequence() + recordsBuilder.numRecords() - 1;
    }

    public boolean hasSequence() {
        return baseSequence() != RecordBatch.NO_SEQUENCE;
    }

    public boolean isTransactional() {
        return recordsBuilder.isTransactional();
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }
}
