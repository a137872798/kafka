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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Higher level consumer access to the network layer with basic support for request futures. This class
 * is thread-safe, but provides no synchronization for response callbacks. This guarantees that no locks
 * are held when they are invoked.
 */
public class ConsumerNetworkClient implements Closeable {
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    private final Logger log;
    /**
     * 内置了networkClient  原本是通过sender线程自动聚合数据 然后设置到networkClient底层的selectable中
     */
    private final KafkaClient client;
    /**
     * 管理所有待发送的请求
     */
    private final UnsentRequests unsent = new UnsentRequests();
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;
    private final int maxPollTimeoutMs;
    private final int requestTimeoutMs;
    private final AtomicBoolean wakeupDisabled = new AtomicBoolean();

    // We do not need high throughput, so use a fair lock to try to avoid starvation
    private final ReentrantLock lock = new ReentrantLock(true);

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.
    // 在这里会维护所有已经收到结果的handler 在io线程中会发现并进行处理
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    /**
     * 存储所有需要在之后断开连接的节点
     */
    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    /**
     * 消费者对基础的client做了增强
     * @param logContext
     * @param client
     * @param metadata
     * @param time
     * @param retryBackoffMs
     * @param requestTimeoutMs
     * @param maxPollTimeoutMs
     */
    public ConsumerNetworkClient(LogContext logContext,
                                 KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 int requestTimeoutMs,
                                 int maxPollTimeoutMs) {
        this.log = logContext.logger(ConsumerNetworkClient.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int defaultRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    /**
     * Send a request with the default timeout. See {@link #send(Node, AbstractRequest.Builder, int)}.
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        return send(node, requestBuilder, requestTimeoutMs);
    }

    /**
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(Timer)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     *
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @param requestTimeoutMs Maximum time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may be cancelled sooner if the socket disconnects
     *                         for any reason.
     * @return A future which indicates the result of the send.
     * 发送请求到某个node
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              AbstractRequest.Builder<?> requestBuilder,
                                              int requestTimeoutMs) {
        long now = time.milliseconds();
        // 该结果用于处理收到的response
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
            requestTimeoutMs, completionHandler);
        // 将待发送的请求设置到unsent中
        unsent.put(node, clientRequest);

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        // 唤醒io线程 io线程会发现某个node有待发送的数据 就会设置到kafkaChannel的send中 同时注册write事件
        client.wakeup();
        return completionHandler.future;
    }

    public Node leastLoadedNode() {
        lock.lock();
        try {
            return client.leastLoadedNode(time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    public boolean hasReadyNodes(long now) {
        lock.lock();
        try {
            return client.hasReadyNodes(now);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     *
     * @return true if update succeeded, false otherwise.
     * 等待元数据更新
     */
    public boolean awaitMetadataUpdate(Timer timer) {
        int version = this.metadata.requestUpdate();
        do {
            poll(timer);
        } while (this.metadata.updateVersion() == version && timer.notExpired());
        return this.metadata.updateVersion() > version;
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     * 确保此时元数据已经更新完毕
     */
    boolean ensureFreshMetadata(Timer timer) {
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(timer.currentTimeMs()) == 0) {
            return awaitMetadataUpdate(timer);
        } else {
            // the metadata is already fresh
            // 此时不需要更新元数据 既没有打上需要更新的标记 也还没有需要刷新的时间
            return true;
        }
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     */
    public void wakeup() {
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is thread-safe
        log.debug("Received user wakeup");
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        while (!future.isDone())
            poll(time.timer(Long.MAX_VALUE), future);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * @param future The request future to wait for
     * @param timer Timer bounding how long this method can block
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     * 只要当future得到了结果就从循环中退出  最大的等待时间是timer
     */
    public boolean poll(RequestFuture<?> future, Timer timer) {
        do {
            poll(timer, future);
            // 不断自旋 等到获取到coordinator后 才能开始拉取数据
        } while (!future.isDone() && timer.notExpired());
        // 根据是否超时返回结果
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(Timer timer) {
        poll(timer, null);
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition  该接口能返回是否还需要等待的标记
     */
    public void poll(Timer timer, PollCondition pollCondition) {
        poll(timer, pollCondition, false);
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     * @param disableWakeup If TRUE disable triggering wake-ups
     *                      包装networkClient,外部通过调用该poll间接执行networkClient的事件循环
     *                      而作为消费者也是以这里作为整个拉取数据的入口
     */
    public void poll(Timer timer, PollCondition pollCondition, boolean disableWakeup) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        // 发送的结果在收到响应后 会将对应的handler存储在这个队列中 等到下一轮开始处理
        firePendingCompletedRequests();

        lock.lock();
        try {
            // Handle async disconnects prior to attempting any sends
            // 在执行拉取任务前会检测coordinator的连接是否可用 如果不可用就会将它设置到待关闭列表中 在这里进行关闭
            handlePendingDisconnects();

            // send all the requests we can send now
            // 找到所有满足发送条件的数据体 进行发送 (设置到kafkaChannel.send上)
            long pollDelayMs = trySend(timer.currentTimeMs());

            // check whether the poll is still needed by the caller. Note that if the expected completion
            // condition becomes satisfied after the call to shouldBlock() (because of a fired completion
            // handler), the client will be woken up.
            // 这种情况代表此时还未获取到coordinator地址信息   client.poll就是处理数据流接收和数据流发送的逻辑 以及触发response回调
            if (pendingCompletion.isEmpty() && (pollCondition == null || pollCondition.shouldBlock())) {
                // if there are no requests in flight, do not block longer than the retry backoff
                long pollTimeout = Math.min(timer.remainingMs(), pollDelayMs);
                if (client.inFlightRequestCount() == 0)
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);
                client.poll(pollTimeout, timer.currentTimeMs());
            } else {
                client.poll(0, timer.currentTimeMs());
            }
            // 更新当前时间
            timer.update();

            // handle any disconnects by failing the active requests. note that disconnects must
            // be checked immediately following poll since any subsequent call to client.ready()
            // will reset the disconnect status
            // 检测是否发现了一些连接失败的node 并处理对于该节点还未发送的req
            checkDisconnects(timer.currentTimeMs());
            // 是否需要检测wakeup标记 如果发现了wakeup标记为true 就会抛出异常终止本次poll
            if (!disableWakeup) {
                // trigger wakeups after checking for disconnects so that the callbacks will be ready
                // to be fired on the next call to poll()
                maybeTriggerWakeup();
            }
            // throw InterruptException if this thread is interrupted
            maybeThrowInterruptException();

            // try again to send requests since buffer space may have been
            // cleared or a connect finished in the poll
            // 经过了上面的poll调用后 可能又有新的node的连接已经完成了 所以这里继续尝试设置send
            trySend(timer.currentTimeMs());

            // fail requests that couldn't be sent if they have expired
            // 找到所有超时未发送的请求 并以超时情况触发回调
            failExpiredRequests(timer.currentTimeMs());

            // clean unsent requests collection to keep the map from growing indefinitely
            unsent.clean();
        } finally {
            lock.unlock();
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        // 经过poll后可能收到了新的结果 执行handler
        firePendingCompletedRequests();

        metadata.maybeThrowAnyException();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     */
    public void pollNoWakeup() {
        poll(time.timer(0), null, true);
    }

    /**
     * Poll for network IO in best-effort only trying to transmit the ready-to-send request
     * Do not check any pending requests or metadata errors so that no exception should ever
     * be thrown, also no wakeups be triggered and no interrupted exception either.
     */
    public void transmitSends() {
        Timer timer = time.timer(0);

        // do not try to handle any disconnects, prev request failures, metadata exception etc;
        // just try once and return immediately
        lock.lock();
        try {
            // send all the requests we can send now
            trySend(timer.currentTimeMs());

            client.poll(0, timer.currentTimeMs());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block until all pending requests from the given node have finished.
     * @param node The node to await requests from
     * @param timer Timer bounding how long this method can block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, Timer timer) {
        while (hasPendingRequests(node) && timer.notExpired()) {
            poll(timer);
        }
        return !hasPendingRequests(node);
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        lock.lock();
        try {
            return unsent.requestCount(node) + client.inFlightRequestCount(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests(Node node) {
        if (unsent.hasRequests(node))
            return true;
        lock.lock();
        try {
            return client.hasInFlightRequests(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        lock.lock();
        try {
            return unsent.requestCount() + client.inFlightRequestCount();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests() {
        if (unsent.hasRequests())
            return true;
        lock.lock();
        try {
            return client.hasInFlightRequests();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 使用回调处理之前收到的所有响应结果
     */
    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;

            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        // wakeup the client in case it is blocking in poll for this future's completion
        // 有几条线程在调用poll? 该方法如果是由io线程调用 就不存在唤醒自己的逻辑了 所以该方法可能会被外部线程调用 唤醒阻塞在底层selectable的io线程
        if (completedRequestsFired)
            client.wakeup();
    }

    /**
     * 针对连接失败的node 以失败结果触发回调
     * @param now
     */
    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        for (Node node : unsent.nodes()) {
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                Collection<ClientRequest> requests = unsent.remove(node);
                for (ClientRequest request : requests) {
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    AuthenticationException authenticationException = client.authenticationException(node);
                    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                            request.callback(), request.destination(), request.createdTimeMs(), now, true,
                            null, authenticationException, null));
                }
            }
        }
    }

    /**
     * 关闭与旧的coordinator的连接
     */
    private void handlePendingDisconnects() {
        lock.lock();
        try {
            while (true) {
                Node node = pendingDisconnects.poll();
                if (node == null)
                    break;

                // 此时可能正好有一组请求还未发往该节点 需要将这些请求以失败形式触发
                failUnsentRequests(node, DisconnectException.INSTANCE);
                // 调用底层的断连逻辑
                client.disconnect(node.idString());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 与某个节点断开连接
     * @param node
     */
    public void disconnectAsync(Node node) {
        // 也是存储队列 并交由io线程处理
        pendingDisconnects.offer(node);
        // 唤醒io线程
        client.wakeup();
    }

    /**
     * 检测超时的请求
     * @param now
     */
    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        Collection<ClientRequest> expiredRequests = unsent.removeExpiredRequests(now);
        for (ClientRequest request : expiredRequests) {
            RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
            handler.onFailure(new TimeoutException("Failed to send request after " + request.requestTimeoutMs() + " ms."));
        }
    }

    /**
     * 以失败方式 关闭发往本节点的所有请求
     * @param node
     * @param e
     */
    private void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        lock.lock();
        try {
            Collection<ClientRequest> unsentRequests = unsent.remove(node);
            for (ClientRequest unsentRequest : unsentRequests) {
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                handler.onFailure(e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将unsent内的数据通过下层client进行发送
     * @param now
     * @return
     */
    long trySend(long now) {
        // 拉取数据允许等待的最大时长
        long pollDelayMs = maxPollTimeoutMs;

        // send any requests that can be sent now
        for (Node node : unsent.nodes()) {
            // 这里unsent中除了与coordinator的交互外 就是与其他server交互获取消息的请求
            Iterator<ClientRequest> iterator = unsent.requestIterator(node);
            if (iterator.hasNext())
                // 此时不一定已经与本node建立连接 可能断连了(在client中没有发送心跳逻辑 可能是认为kafka作为一个数据量很大的消息中间件发送间隔应当很短 不需要心跳来增加额外的负担
                // 而由于与zk的节点交互没有这么频繁 所以需要心跳检测)
                pollDelayMs = Math.min(pollDelayMs, client.pollDelayMs(node, now));

            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                // 如果与该node的连接已经准备完毕 将请求发送到node  (实际上在下层还有一个inflight队列)
                // 应该是有个机制能够确保队列中只有一个请求 不然底层连续设置send对象会报错的 也就是只有在收到结果后才会发送下一个请求
                if (client.ready(node, now)) {
                    client.send(request, now);
                    iterator.remove();
                } else {
                    // try next node when current node is not ready
                    break;
                }
            }
        }
        return pollDelayMs;
    }

    /**
     * 设置了需要从poll中唤醒的标记 抛出异常
     */
    public void maybeTriggerWakeup() {
        if (!wakeupDisabled.get() && wakeup.get()) {
            log.debug("Raising WakeupException in response to user wakeup");
            wakeup.set(false);
            throw new WakeupException();
        }
    }

    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        wakeupDisabled.set(true);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            client.close();
        } finally {
            lock.unlock();
        }
    }


    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     * 检测与某个node的连接是否可用
     */
    public boolean isUnavailable(Node node) {
        lock.lock();
        try {
            return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check for an authentication error on a given node and raise the exception if there is one.
     */
    public void maybeThrowAuthFailure(Node node) {
        lock.lock();
        try {
            AuthenticationException exception = client.authenticationException(node);
            if (exception != null)
                throw exception;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, AbstractRequest.Builder)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        lock.lock();
        try {
            client.ready(node, time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 该handler对象并没有直接处理response 而是做了增强 用户可以自己监听内部的future对象 并不断追加监听器，以实现拓展需求
     */
    private class RequestFutureCompletionHandler implements RequestCompletionHandler {

        /**
         * 当收到结果后就会设置到future中
         */
        private final RequestFuture<ClientResponse> future;
        /**
         * 收到的原始结果
         */
        private ClientResponse response;
        private RuntimeException e;

        private RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        /**
         * 在io线程中会挨个处理pendingCompletion的每个对象 并将结果设置到future中
         */
        public void fireCompletion() {
            // 当收到了直观的异常时 直接触发raise
            if (e != null) {
                future.raise(e);
                // 如果发现在response中包含了其他异常 进行设置
            } else if (response.authenticationException() != null) {
                future.raise(response.authenticationException());
            } else if (response.wasDisconnected()) {
                log.debug("Cancelled request with header {} due to node {} being disconnected",
                        response.requestHeader(), response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                // 代表本次调用成功 设置结果
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            pendingCompletion.add(this);
        }

        @Override
        public void onComplete(ClientResponse response) {
            this.response = response;
            pendingCompletion.add(this);
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We therefore
     * introduce this interface to push the condition checking as close as possible to the invocation
     * of poll. In particular, the check will be done while holding the lock used to protect concurrent
     * access to {@link org.apache.kafka.clients.NetworkClient}, which means implementations must be
     * very careful about locking order if the callback must acquire additional locks.
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

    /*
     * A thread-safe helper class to hold requests per node that have not been sent yet
     * 以线程安全的方式管理所有未发送的请求
     */
    private static final class UnsentRequests {

        /**
         * 本对象负责管理发往每个节点的请求对象
         */
        private final ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent;

        private UnsentRequests() {
            unsent = new ConcurrentHashMap<>();
        }

        /**
         * 为某个node追加一个新的请求
         * @param node
         * @param request
         */
        public void put(Node node, ClientRequest request) {
            // the lock protects the put from a concurrent removal of the queue for the node
            synchronized (unsent) {
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.computeIfAbsent(node, key -> new ConcurrentLinkedQueue<>());
                requests.add(request);
            }
        }

        public int requestCount(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? 0 : requests.size();
        }

        public int requestCount() {
            int total = 0;
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                total += requests.size();
            return total;
        }

        public boolean hasRequests(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests != null && !requests.isEmpty();
        }

        public boolean hasRequests() {
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                if (!requests.isEmpty())
                    return true;
            return false;
        }

        /**
         * 找到所有超时请求
         * @param now
         * @return
         */
        private Collection<ClientRequest> removeExpiredRequests(long now) {
            List<ClientRequest> expiredRequests = new ArrayList<>();
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                Iterator<ClientRequest> requestIterator = requests.iterator();
                while (requestIterator.hasNext()) {
                    ClientRequest request = requestIterator.next();
                    long elapsedMs = Math.max(0, now - request.createdTimeMs());
                    if (elapsedMs > request.requestTimeoutMs()) {
                        expiredRequests.add(request);
                        requestIterator.remove();
                    } else
                        break;
                }
            }
            return expiredRequests;
        }

        public void clean() {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                Iterator<ConcurrentLinkedQueue<ClientRequest>> iterator = unsent.values().iterator();
                while (iterator.hasNext()) {
                    ConcurrentLinkedQueue<ClientRequest> requests = iterator.next();
                    if (requests.isEmpty())
                        iterator.remove();
                }
            }
        }

        public Collection<ClientRequest> remove(Node node) {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.remove(node);
                return requests == null ? Collections.<ClientRequest>emptyList() : requests;
            }
        }

        public Iterator<ClientRequest> requestIterator(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? Collections.<ClientRequest>emptyIterator() : requests.iterator();
        }

        public Collection<Node> nodes() {
            return unsent.keySet();
        }
    }

}
