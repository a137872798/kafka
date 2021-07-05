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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MultiRecordsSend;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * This class provides a way to build {@link Send} objects for network transmission
 * from generated {@link org.apache.kafka.common.protocol.ApiMessage} types without
 * allocating new space for "zero-copy" fields (see {@link #writeByteBuffer(ByteBuffer)}
 * and {@link #writeRecords(BaseRecords)}).
 *
 * See {@link org.apache.kafka.common.requests.EnvelopeRequest#toSend(RequestHeader)}
 * for example usage.
 */
public class SendBuilder implements Writable {
    private final ByteBuffer buffer;

    private final Queue<Send> sends = new ArrayDeque<>(1);
    /**
     * sends内数据的总大小
     */
    private long sizeOfSends = 0;

    private final List<ByteBuffer> buffers = new ArrayList<>();
    private long sizeOfBuffers = 0;

    SendBuilder(int size) {
        this.buffer = ByteBuffer.allocate(size);
        this.buffer.mark();
    }

    @Override
    public void writeByte(byte val) {
        buffer.put(val);
    }

    @Override
    public void writeShort(short val) {
        buffer.putShort(val);
    }

    @Override
    public void writeInt(int val) {
        buffer.putInt(val);
    }

    @Override
    public void writeLong(long val) {
        buffer.putLong(val);
    }

    @Override
    public void writeDouble(double val) {
        buffer.putDouble(val);
    }

    @Override
    public void writeByteArray(byte[] arr) {
        buffer.put(arr);
    }

    @Override
    public void writeUnsignedVarint(int i) {
        ByteUtils.writeUnsignedVarint(i, buffer);
    }

    /**
     * Write a byte buffer. The reference to the underlying buffer will
     * be retained in the result of {@link #build()}.
     *
     * @param buf the buffer to write
     */
    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        flushPendingBuffer();
        addBuffer(buf.duplicate());
    }

    @Override
    public void writeVarint(int i) {
        ByteUtils.writeVarint(i, buffer);
    }

    @Override
    public void writeVarlong(long i) {
        ByteUtils.writeVarlong(i, buffer);
    }

    /**
     * 将某个buf对象添加到 buffers中
     * @param buffer
     */
    private void addBuffer(ByteBuffer buffer) {
        buffers.add(buffer);
        sizeOfBuffers += buffer.remaining();
    }

    /**
     * 添加一个新的send对象
     * @param send
     */
    private void addSend(Send send) {
        sends.add(send);
        sizeOfSends += send.size();
    }

    /**
     * 清空buffer中的数据
     */
    private void clearBuffers() {
        buffers.clear();
        sizeOfBuffers = 0;
    }

    /**
     * Write a record set. The underlying record data will be retained
     * in the result of {@link #build()}. See {@link BaseRecords#toSend()}.
     *
     * @param records the records to write
     */
    @Override
    public void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            flushPendingBuffer();
            addBuffer(((MemoryRecords) records).buffer());
        } else if (records instanceof UnalignedMemoryRecords) {
            flushPendingBuffer();
            addBuffer(((UnalignedMemoryRecords) records).buffer());
        } else {
            flushPendingSend();
            addSend(records.toSend());
        }
    }

    /**
     * 一开始message的数据是写入到缓冲区中的  这里将数据移动到其他地方
     */
    private void flushPendingSend() {
        // 将buffer的分片添加到buffers中
        flushPendingBuffer();
        // 此时还有囤积的数据将它们添加到send中
        if (!buffers.isEmpty()) {
            ByteBuffer[] byteBufferArray = buffers.toArray(new ByteBuffer[0]);
            // 将本批数据包装成一个 ByteBufferSend  sizeOfBuffers代表buffers中的字节数
            addSend(new ByteBufferSend(byteBufferArray, sizeOfBuffers));
            // 清理所有buffer数据
            clearBuffers();
        }
    }

    /**
     * 为当前缓冲区中的数据生成分片 并存入到buffers中
     */
    private void flushPendingBuffer() {
        int latestPosition = buffer.position();
        // 回到之前mark的位置 首次调用默认是0
        buffer.reset();

        // 代表距离上次有新的数据写入
        if (latestPosition > buffer.position()) {
            buffer.limit(latestPosition);
            // 针对pos->limit部分的数据生成分片 并加入到buffers中
            addBuffer(buffer.slice());

            // mark当前位置 之后的数据写入会以该位置作为起点
            buffer.position(latestPosition);
            buffer.limit(buffer.capacity());
            buffer.mark();
        }
    }

    /**
     * 当header/data的数据都写入到send.builder后 通过该方法生成send对象
     * @return
     */
    public Send build() {
        // 将buffer的数据封装成sends对象 这里使用的是分片 所以不会消耗额外的内存
        flushPendingSend();

        if (sends.size() == 1) {
            // 返回单个send对象
            return sends.poll();
        } else {
            // 如果此时有多个send对象 将他们包装成一个multi对象
            return new MultiRecordsSend(sends, sizeOfSends);
        }
    }

    public static Send buildRequestSend(
        RequestHeader header,
        Message apiRequest
    ) {
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiRequest,
            header.apiVersion()
        );
    }

    public static Send buildResponseSend(
        ResponseHeader header,
        Message apiResponse,
        short apiVersion
    ) {
        return buildSend(
            header.data(),
            header.headerVersion(),
            apiResponse,
            apiVersion
        );
    }

    /**
     * 基于message和data构建send对象
     * @param header
     * @param headerVersion
     * @param apiMessage
     * @param apiVersion
     * @return
     */
    private static Send buildSend(
        Message header,
        short headerVersion,
        Message apiMessage,
        short apiVersion
    ) {
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();

        // 有关消息的长度计算是通过该对象
        MessageSizeAccumulator messageSize = new MessageSizeAccumulator();
        header.addSize(messageSize, serializationCache, headerVersion);
        apiMessage.addSize(messageSize, serializationCache, apiVersion);

        SendBuilder builder = new SendBuilder(messageSize.sizeExcludingZeroCopy() + 4);
        builder.writeInt(messageSize.totalSize());
        header.write(builder, serializationCache, headerVersion);
        apiMessage.write(builder, serializationCache, apiVersion);

        return builder.build();
    }

}
