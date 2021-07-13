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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Result of an asynchronous request from {@link ConsumerNetworkClient}. Use {@link ConsumerNetworkClient#poll(Timer)}
 * (and variants) to finish a request future. Use {@link #isDone()} to check if the future is complete, and
 * {@link #succeeded()} to check if the request completed successfully. Typical usage might look like this:
 *
 * <pre>
 *     RequestFuture<ClientResponse> future = client.send(api, request);
 *     client.poll(future);
 *
 *     if (future.succeeded()) {
 *         ClientResponse response = future.value();
 *         // Handle response
 *     } else {
 *         throw future.exception();
 *     }
 * </pre>
 *
 * @param <T> Return type of the result (Can be Void if there is no response)
 *           PollCondition包含一个是否应该继续阻塞的api
 */
public class RequestFuture<T> implements ConsumerNetworkClient.PollCondition {

    private static final Object INCOMPLETE_SENTINEL = new Object();
    /**
     * 使用该对象存储future的结果
     */
    private final AtomicReference<Object> result = new AtomicReference<>(INCOMPLETE_SENTINEL);
    /**
     * 存储一组处理结果的监听器
     */
    private final ConcurrentLinkedQueue<RequestFutureListener<T>> listeners = new ConcurrentLinkedQueue<>();
    /**
     * 在未设置结果或者exception之前 尝试获取结果的线程都会被本对象阻塞
     */
    private final CountDownLatch completedLatch = new CountDownLatch(1);

    /**
     * Check whether the response is ready to be handled
     * @return true if the response is ready, false otherwise
     * 当获得结果后 会覆盖原来的INCOMPLETE_SENTINEL
     */
    public boolean isDone() {
        return result.get() != INCOMPLETE_SENTINEL;
    }

    /**
     * 通过闭锁实现线程等待
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    public boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        return completedLatch.await(timeout, unit);
    }

    /**
     * Get the value corresponding to this request (only available if the request succeeded)
     * @return the value set in {@link #complete(Object)}
     * @throws IllegalStateException if the future is not complete or failed
     */
    @SuppressWarnings("unchecked")
    public T value() {
        if (!succeeded())
            throw new IllegalStateException("Attempt to retrieve value from future which hasn't successfully completed");
        return (T) result.get();
    }

    /**
     * Check if the request succeeded;
     * @return true if the request completed and was successful
     */
    public boolean succeeded() {
        return isDone() && !failed();
    }

    /**
     * Check if the request failed.
     * @return true if the request completed with a failure
     * 当出现异常时 也是设置到result字段中
     */
    public boolean failed() {
        return result.get() instanceof RuntimeException;
    }

    /**
     * Check if the request is retriable (convenience method for checking if
     * the exception is an instance of {@link RetriableException}.
     * @return true if it is retriable, false otherwise
     * @throws IllegalStateException if the future is not complete or completed successfully
     * 查看异常是否是可重试的
     */
    public boolean isRetriable() {
        return exception() instanceof RetriableException;
    }

    /**
     * Get the exception from a failed result (only available if the request failed)
     * @return the exception set in {@link #raise(RuntimeException)}
     * @throws IllegalStateException if the future is not complete or completed successfully
     */
    public RuntimeException exception() {
        if (!failed())
            throw new IllegalStateException("Attempt to retrieve exception from future which hasn't failed");
        return (RuntimeException) result.get();
    }

    /**
     * Complete the request successfully. After this call, {@link #succeeded()} will return true
     * and the value can be obtained through {@link #value()}.
     * @param value corresponding value (or null if there is none)
     * @throws IllegalStateException if the future has already been completed
     * @throws IllegalArgumentException if the argument is an instance of {@link RuntimeException}
     * 设置future的结果
     */
    public void complete(T value) {
        try {
            if (value instanceof RuntimeException)
                throw new IllegalArgumentException("The argument to complete can not be an instance of RuntimeException");

            if (!result.compareAndSet(INCOMPLETE_SENTINEL, value))
                throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
            fireSuccess();
        } finally {
            completedLatch.countDown();
        }
    }

    /**
     * Raise an exception. The request will be marked as failed, and the caller can either
     * handle the exception or throw it.
     * @param e corresponding exception to be passed to caller
     * @throws IllegalStateException if the future has already been completed
     * 本次结果得到异常信息
     */
    public void raise(RuntimeException e) {
        try {
            if (e == null)
                throw new IllegalArgumentException("The exception passed to raise must not be null");

            if (!result.compareAndSet(INCOMPLETE_SENTINEL, e))
                throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");

            fireFailure();
        } finally {
            completedLatch.countDown();
        }
    }

    /**
     * Raise an error. The request will be marked as failed.
     * @param error corresponding error to be passed to caller
     */
    public void raise(Errors error) {
        raise(error.exception());
    }

    /**
     * future成功得到结果 触发所有监听器
     */
    private void fireSuccess() {
        T value = value();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null)
                break;
            listener.onSuccess(value);
        }
    }

    /**
     * 本次处理失败 触发所有监听器
     */
    private void fireFailure() {
        RuntimeException exception = exception();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null)
                break;
            listener.onFailure(exception);
        }
    }

    /**
     * Add a listener which will be notified when the future completes
     * @param listener non-null listener to add
     */
    public void addListener(RequestFutureListener<T> listener) {
        this.listeners.add(listener);
        if (failed())
            fireFailure();
        else if (succeeded())
            fireSuccess();
    }

    /**
     * Convert from a request future of one type to another type
     * @param adapter The adapter which does the conversion
     * @param <S> The type of the future adapted to
     * @return The new future
     * 基于某个监听器对象 生成一个requestFuture
     */
    public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
        final RequestFuture<S> adapted = new RequestFuture<>();
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                adapter.onSuccess(value, adapted);
            }

            @Override
            public void onFailure(RuntimeException e) {
                adapter.onFailure(e, adapted);
            }
        });
        return adapted;
    }

    /**
     * 当本对象产生结果时 会传播到下面的future
     * @param future
     */
    public void chain(final RequestFuture<T> future) {
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                future.complete(value);
            }

            @Override
            public void onFailure(RuntimeException e) {
                future.raise(e);
            }
        });
    }

    public static <T> RequestFuture<T> failure(RuntimeException e) {
        RequestFuture<T> future = new RequestFuture<>();
        future.raise(e);
        return future;
    }

    /**
     * 本次只是表示成功 但是没有实际结果
     * @return
     */
    public static RequestFuture<Void> voidSuccess() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        return future;
    }

    /**
     * 下面是几个特殊的异常
     * @param <T>
     * @return
     */
    public static <T> RequestFuture<T> coordinatorNotAvailable() {
        return failure(Errors.COORDINATOR_NOT_AVAILABLE.exception());
    }

    public static <T> RequestFuture<T> noBrokersAvailable() {
        return failure(new NoAvailableBrokersException());
    }

    /**
     * 只要结果还未设置 就代表还需要继续等待
     * @return
     */
    @Override
    public boolean shouldBlock() {
        return !isDone();
    }
}
