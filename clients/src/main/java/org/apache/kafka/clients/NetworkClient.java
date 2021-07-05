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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CorrelationIdMismatchException;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 * 作为kafka的客户端负责与各个node建立连接以及发送请求
 */
public class NetworkClient implements KafkaClient {

    /**
     * 描述当前client的状态
     */
    private enum State {
        // 激活 关闭中 已关闭
        ACTIVE,
        CLOSING,
        CLOSED
    }

    private final Logger log;

    /* the selector used to perform network i/o
     * 使用该选择器实现非阻塞io
     * */
    private final Selectable selector;

    /**
     * 该对象负责更新元数据信息 作为client需要知道集群中存在哪些node以及它们的地址信息
     * 会有一个定期刷新的机制
     */
    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection
     * 该对象负责维护与其它节点的连接状态信息
     * */
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response
     * 维护已经发送的还未收到响应结果的所有req对象
     * */
    private final InFlightRequests inFlightRequests;

    // 这里是默认的缓冲区大小
    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;
    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* default timeout for individual requests to await acknowledgement from servers
    * 当发出某个请求时 默认的超时时间 超过该时间没有收到结果就认为处理失败
    * */
    private final int defaultRequestTimeoutMs;

    /* time in ms to wait before retrying to create connection to a server
    * 当无连接可用时 存在一个补偿时间 之后才会进行重试
    * */
    private final long reconnectBackoffMs;

    /**
     * 描述采用的dns解析方式
     */
    private final ClientDnsLookup clientDnsLookup;

    /**
     * 内部包含一些时间相关的函数
     */
    private final Time time;

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    private final boolean discoverBrokerVersions;

    /**
     * 存储集群中其他节点的版本信息
     */
    private final ApiVersions apiVersions;

    /**
     * 存储获取版本号req的容器
     */
    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();

    private final List<ClientResponse> abortedSends = new LinkedList<>();

    /**
     * 该对象内部存储的是统计信息 先忽略
     */
    private final Sensor throttleTimeSensor;

    /**
     * 描述当前client的状态
     */
    private final AtomicReference<State> state;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         ClientDnsLookup clientDnsLookup,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(selector,
                metadata,
                clientId,
                maxInFlightRequestsPerConnection,
                reconnectBackoffMs,
                reconnectBackoffMax,
                socketSendBuffer,
                socketReceiveBuffer,
                defaultRequestTimeoutMs,
                connectionSetupTimeoutMs,
                connectionSetupTimeoutMaxMs,
                clientDnsLookup,
                time,
                discoverBrokerVersions,
                apiVersions,
                null,
                logContext);
    }

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         ClientDnsLookup clientDnsLookup,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext) {
        this(null,
                metadata,
                selector,
                clientId,
                maxInFlightRequestsPerConnection,
                reconnectBackoffMs,
                reconnectBackoffMax,
                socketSendBuffer,
                socketReceiveBuffer,
                defaultRequestTimeoutMs,
                connectionSetupTimeoutMs,
                connectionSetupTimeoutMaxMs,
                clientDnsLookup,
                time,
                discoverBrokerVersions,
                apiVersions,
                throttleTimeSensor,
                logContext,
                new DefaultHostResolver());
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         ClientDnsLookup clientDnsLookup,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(metadataUpdater,
                null,
                selector,
                clientId,
                maxInFlightRequestsPerConnection,
                reconnectBackoffMs,
                reconnectBackoffMax,
                socketSendBuffer,
                socketReceiveBuffer,
                defaultRequestTimeoutMs,
                connectionSetupTimeoutMs,
                connectionSetupTimeoutMaxMs,
                clientDnsLookup,
                time,
                discoverBrokerVersions,
                apiVersions,
                null,
                logContext,
                new DefaultHostResolver());
    }

    /**
     * 初始化client对象
     * @param metadataUpdater 该对象定义了如何更新元数据
     * @param metadata 创建client时传入的初始元数据对象
     * @param selector
     * @param clientId
     * @param maxInFlightRequestsPerConnection
     * @param reconnectBackoffMs
     * @param reconnectBackoffMax
     * @param socketSendBuffer
     * @param socketReceiveBuffer
     * @param defaultRequestTimeoutMs
     * @param connectionSetupTimeoutMs
     * @param connectionSetupTimeoutMaxMs
     * @param clientDnsLookup
     * @param time
     * @param discoverBrokerVersions
     * @param apiVersions
     * @param throttleTimeSensor
     * @param logContext
     * @param hostResolver
     */
    public NetworkClient(MetadataUpdater metadataUpdater,
                         Metadata metadata,
                         Selectable selector,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         ClientDnsLookup clientDnsLookup,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext,
                         HostResolver hostResolver) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         * 当元数据更新对象未传入时 要求metadata必须传入  因为要创建的默认updater对象在初始化时要求metadata有效
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        // 相关对象的初始化
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(
                reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, logContext, hostResolver);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
        this.clientDnsLookup = clientDnsLookup;
        this.state = new AtomicReference<>(State.ACTIVE);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now  The current timestamp
     * @return True if we are ready to send to the given node
     * 准备与某个node建立连接
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        // 如果已经与该节点建立连接了 返回true  如果此时与该节点囤积了很多请求 也会返回false
        if (isReady(node, now))
            return true;

        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            // 建立连接
            initiateConnect(node, now);

        return false;
    }

    // Visible for testing
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     *               与某个节点断开连接
     */
    @Override
    public void disconnect(String nodeId) {
        // 连接已经断开 不需要处理
        if (connectionStates.isDisconnected(nodeId))
            return;

        // 通过底层nio对象关闭连接
        selector.close(nodeId);
        long now = time.milliseconds();

        cancelInFlightRequests(nodeId, now, abortedSends);

        connectionStates.disconnected(nodeId, now);

        if (log.isTraceEnabled()) {
            log.trace("Manually disconnected from {}. Aborted in-flight requests: {}.", nodeId, inFlightRequests);
        }
    }

    private void cancelInFlightRequests(String nodeId, long now, Collection<ClientResponse> responses) {
        Iterable<InFlightRequest> inFlightRequests = this.inFlightRequests.clearAll(nodeId);
        for (InFlightRequest request : inFlightRequests) {
            log.trace("Cancelled request {} {} with correlation id {} due to node {} being disconnected",
                    request.header.apiKey(), request.request, request.header.correlationId(), nodeId);

            if (!request.isInternalRequest) {
                if (responses != null)
                    responses.add(request.disconnected(now, null));
            } else if (request.header.apiKey() == ApiKeys.METADATA) {
                metadataUpdater.handleFailedRequest(now, Optional.empty());
            }
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        long now = time.milliseconds();
        cancelInFlightRequests(nodeId, now, null);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now  The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    // Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
    // This is for testing.
    public long throttleDelayMs(Node node, long now) {
        return connectionStates.throttleDelayMs(node.idString(), now);
    }

    /**
     * Return the poll delay in milliseconds based on both connection and throttle delay.
     *
     * @param node the connection to check
     * @param now  the current time in ms
     */
    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionStates.pollDelayMs(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now  The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now);
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     * @param now  the current timestamp
     *             判断能否往某个node发送请求
     */
    private boolean canSendRequest(String node, long now) {
        // 先判断与该node是否已经建立连接  之后判断channel是否准备完毕(channel已经初始化完毕 并且完成认证之类的任务) 最后判断是否有很多请求囤积在该node上
        return connectionStates.isReady(node, now) && selector.isChannelReady(node) &&
                inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     *
     * @param request The request
     * @param now     The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    /**
     * 发送更新元数据的请求
     * @param builder
     * @param nodeConnectionId
     * @param now
     */
    void sendInternalMetadataRequest(MetadataRequest.Builder builder, String nodeConnectionId, long now) {
        // 构建请求体
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        // 通过网络模块(selector)发送数据
        doSend(clientRequest, true, now);
    }

    /**
     * 发送某个req对象
     * @param clientRequest
     * @param isInternalRequest  本次请求是否是kafka内部请求
     * @param now
     */
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        ensureActive();
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            // 如果是内部请求 在前面的逻辑中应该已经调用过canSendRequest  所以这里仅针对外部请求做判断
            if (!canSendRequest(nodeId, now))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            // 获取该node的版本号信息
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                // 生成请求体的兼容版本信息
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            // 发送请求
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException unsupportedVersionException) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException);
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, unsupportedVersionException, null, null);

            if (!isInternalRequest)
                abortedSends.add(clientResponse);
            else if (clientRequest.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
        }
    }

    /**
     * 将req包装成send对象后 通过网络模块发送
     * @param clientRequest
     * @param isInternalRequest
     * @param now
     * @param request
     */
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        String destination = clientRequest.destination();
        // 基于版本信息构建请求头
        RequestHeader header = clientRequest.makeHeader(request.version());
        if (log.isDebugEnabled()) {
            log.debug("Sending {} request with header {} and timeout {} to node {}: {}",
                    clientRequest.apiKey(), header, clientRequest.requestTimeoutMs(), destination, request);
        }
        // 将数据体转换成send对象  send对象就是将req的数据转换成字节数组
        Send send = request.toSend(header);
        // 构建inflight请求对象
        InFlightRequest inFlightRequest = new InFlightRequest(
                clientRequest,
                header,
                isInternalRequest,
                request,
                send,
                now);
        // 将请求添加到容器中 并且通过网络模块发送数据
        this.inFlightRequests.add(inFlightRequest);
        selector.send(new NetworkSend(clientRequest.destination(), send));
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now     The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        ensureActive();

        if (!abortedSends.isEmpty()) {
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            List<ClientResponse> responses = new ArrayList<>();
            handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            this.selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        handleCompletedSends(responses, updatedNow);
        handleCompletedReceives(responses, updatedNow);
        handleDisconnections(responses, updatedNow);
        handleConnections();
        handleInitiateApiVersionRequests(updatedNow);
        handleTimedOutConnections(responses, updatedNow);
        handleTimedOutRequests(responses, updatedNow);
        completeResponses(responses);

        return responses;
    }

    private void completeResponses(List<ClientResponse> responses) {
        for (ClientResponse response : responses) {
            try {
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    @Override
    public boolean hasReadyNodes(long now) {
        return connectionStates.hasReadyNodes(now);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    @Override
    public void initiateClose() {
        if (state.compareAndSet(State.ACTIVE, State.CLOSING)) {
            wakeup();
        }
    }

    @Override
    public boolean active() {
        return state.get() == State.ACTIVE;
    }

    private void ensureActive() {
        if (!active())
            throw new DisconnectException("NetworkClient is no longer active, state is " + state);
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        state.compareAndSet(State.ACTIVE, State.CLOSING);
        if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
            this.selector.close();
            this.metadataUpdater.close();
        } else {
            log.warn("Attempting to close NetworkClient that has already been closed.");
        }
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. If no connection exists, this method will prefer a node
     * with least recent connection attempts. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period, or an active
     * connection which is being throttled.
     *
     * @return The node with the fewest in-flight requests.
     * 找到一个此时负载最小的server节点
     */
    @Override
    public Node leastLoadedNode(long now) {
        // 默认情况下是从缓存中获取当前观测到的所有server
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        if (nodes.isEmpty())
            throw new IllegalStateException("There are no nodes in the Kafka cluster");
        int inflight = Integer.MAX_VALUE;

        Node foundConnecting = null;
        Node foundCanConnect = null;
        Node foundReady = null;

        // 生成一个随机偏移量作为起始下标 使得集群中每个client的请求被负载到不同的server
        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            // 检测能否往该节点发送请求
            if (canSendRequest(node.idString(), now)) {
                // 对于所有能发送请求的节点 再根据此时飞行中的node数量排序
                int currInflight = this.inFlightRequests.count(node.idString());
                // 如果该node还没有任何飞行中请求 直接确定使用这个node
                if (currInflight == 0) {
                    // if we find an established connection with no in-flight requests we can stop right away
                    log.trace("Found least loaded node {} connected with no in-flight requests", node);
                    return node;
                    // 使用这个更优的节点
                } else if (currInflight < inflight) {
                    // otherwise if this is the best we have found so far, record that
                    inflight = currInflight;
                    foundReady = node;
                }
            } else if (connectionStates.isPreparingConnection(node.idString())) {
                foundConnecting = node;
                // 代表还未开始连接 并且此时允许建立连接
            } else if (canConnect(node, now)) {
                if (foundCanConnect == null ||
                        this.connectionStates.lastConnectAttemptMs(foundCanConnect.idString()) >
                                this.connectionStates.lastConnectAttemptMs(node.idString())) {
                    foundCanConnect = node;
                }
            } else {
                log.trace("Removing node {} from least loaded node selection since it is neither ready " +
                        "for sending or connecting", node);
            }
        }

        // 按优先级返回node
        // We prefer established connections if possible. Otherwise, we will wait for connections
        // which are being established before connecting to new nodes.
        if (foundReady != null) {
            log.trace("Found least loaded node {} with {} inflight requests", foundReady, inflight);
            return foundReady;
        } else if (foundConnecting != null) {
            log.trace("Found least loaded connecting node {}", foundConnecting);
            return foundConnecting;
        } else if (foundCanConnect != null) {
            log.trace("Found least loaded node {} with no active connection", foundCanConnect);
            return foundCanConnect;
        } else {
            log.trace("Least loaded node selection failed to find an available node");
            return null;
        }
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        try {
            return AbstractResponse.parseResponse(responseBuffer, requestHeader);
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing response for request with header " + requestHeader, e);
        } catch (CorrelationIdMismatchException e) {
            if (SaslClientAuthenticator.isReserved(requestHeader.correlationId())
                    && !SaslClientAuthenticator.isReserved(e.responseCorrelationId()))
                throw new SchemaException("The response is unrelated to Sasl request since its correlation id is "
                        + e.responseCorrelationId() + " and the reserved range for Sasl request is [ "
                        + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + ","
                        + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]");
            else {
                throw e;
            }
        }
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses       The list of responses to update
     * @param nodeId          Id of the node to be disconnected
     * @param now             The current time
     * @param disconnectState The state of the disconnected channel
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState) {
        connectionStates.disconnected(nodeId, now);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
        switch (disconnectState.state()) {
            case AUTHENTICATION_FAILED:
                AuthenticationException exception = disconnectState.exception();
                connectionStates.authenticationFailed(nodeId, now, exception);
                log.error("Connection to node {} ({}) failed authentication due to: {}", nodeId,
                        disconnectState.remoteAddress(), exception.getMessage());
                break;
            case AUTHENTICATE:
                log.warn("Connection to node {} ({}) terminated during authentication. This may happen " +
                                "due to any of the following reasons: (1) Authentication failed due to invalid " +
                                "credentials with brokers older than 1.0.0, (2) Firewall blocking Kafka TLS " +
                                "traffic (eg it may only allow HTTPS traffic), (3) Transient network issue.",
                        nodeId, disconnectState.remoteAddress());
                break;
            case NOT_CONNECTED:
                log.warn("Connection to node {} ({}) could not be established. Broker may not be available.", nodeId, disconnectState.remoteAddress());
                break;
            default:
                break; // Disconnections in other states are logged at debug level in Selector
        }

        cancelInFlightRequests(nodeId, now, responses);
        metadataUpdater.handleServerDisconnect(now, nodeId, Optional.ofNullable(disconnectState.exception()));
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now       The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        List<String> nodeIds = this.inFlightRequests.nodesWithTimedOutRequests(now);
        for (String nodeId : nodeIds) {
            // close connection to the node
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }
    }

    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * Handle socket channel connection timeout. The timeout will hit iff a connection
     * stays at the ConnectionState.CONNECTING state longer than the timeout value,
     * as indicated by ClusterConnectionStates.NodeConnectionState.
     *
     * @param responses The list of responses to update
     * @param now       The current time
     */
    private void handleTimedOutConnections(List<ClientResponse> responses, long now) {
        List<String> nodes = connectionStates.nodesWithConnectionSetupTimeout(now);
        for (String nodeId : nodes) {
            this.selector.close(nodeId);
            log.debug(
                    "Disconnecting from node {} due to socket connection setup timeout. " +
                            "The timeout value is {} ms.",
                    nodeId,
                    connectionStates.connectionSetupTimeoutMs(nodeId));
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now       The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (NetworkSend send : this.selector.completedSends()) {
            InFlightRequest request = this.inFlightRequests.lastSent(send.destinationId());
            if (!request.expectResponse) {
                this.inFlightRequests.completeLastSent(send.destinationId());
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * If a response from a node includes a non-zero throttle delay and client-side throttling has been enabled for
     * the connection to the node, throttle the connection for the specified delay.
     *
     * @param response   the response
     * @param apiVersion the API version of the response
     * @param nodeId     the id of the node
     * @param now        The current time
     */
    private void maybeThrottle(AbstractResponse response, short apiVersion, String nodeId, long now) {
        int throttleTimeMs = response.throttleTimeMs();
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            connectionStates.throttle(nodeId, now + throttleTimeMs);
            log.trace("Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                    now + throttleTimeMs);
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     *
     * @param responses The list of responses to update
     * @param now       The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            InFlightRequest req = inFlightRequests.completeNext(source);

            AbstractResponse response = parseResponse(receive.payload(), req.header);
            if (throttleTimeSensor != null)
                throttleTimeSensor.record(response.throttleTimeMs(), now);

            if (log.isDebugEnabled()) {
                log.debug("Received {} response from node {} for request with header {}: {}",
                        req.header.apiKey(), req.destination, req.header, response);
            }

            // If the received response includes a throttle delay, throttle the connection.
            maybeThrottle(response, req.header.apiVersion(), req.destination, now);
            if (req.isInternalRequest && response instanceof MetadataResponse)
                metadataUpdater.handleSuccessfulResponse(req.header, now, (MetadataResponse) response);
            else if (req.isInternalRequest && response instanceof ApiVersionsResponse)
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) response);
            else
                responses.add(req.completed(response, now));
        }
    }

    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        final String node = req.destination;
        if (apiVersionsResponse.data().errorCode() != Errors.NONE.code()) {
            if (req.request.version() == 0 || apiVersionsResponse.data().errorCode() != Errors.UNSUPPORTED_VERSION.code()) {
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        Errors.forCode(apiVersionsResponse.data().errorCode()), node, req.header.correlationId());
                this.selector.close(node);
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE);
            } else {
                // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported versions of
                // the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
                // If not provided, the client falls back to version 0.
                short maxApiVersion = 0;
                if (apiVersionsResponse.data().apiKeys().size() > 0) {
                    ApiVersion apiVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
                    if (apiVersion != null) {
                        maxApiVersion = apiVersion.maxVersion();
                    }
                }
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder(maxApiVersion));
            }
            return;
        }
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(apiVersionsResponse.data().apiKeys());
        apiVersions.update(node, nodeVersionInfo);
        this.connectionStates.ready(node);
        log.debug("Node {} has finalized features epoch: {}, finalized features: {}, supported features: {}, API versions: {}.",
                node, apiVersionsResponse.data().finalizedFeaturesEpoch(), apiVersionsResponse.data().finalizedFeatures(),
                apiVersionsResponse.data().supportedFeatures(), nodeVersionInfo);
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now       The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now, entry.getValue());
        }
    }

    /**
     * Record any newly completed connections
     */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            // We are now connected.  Note that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                this.connectionStates.checkingApiVersions(node);
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    private void handleInitiateApiVersionRequests(long now) {
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
                doSend(clientRequest, true, now);
                iter.remove();
            }
        }
    }

    /**
     * Initiate a connection to the given node
     *
     * @param node the node to connect to
     * @param now  current time in epoch milliseconds
     *             与某个指定的节点建立连接
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            // 将node对应的state修改成connection 并加入到相关的容器中
            connectionStates.connecting(nodeConnectionId, now, node.host(), clientDnsLookup);
            // 开始解析地址
            InetAddress address = connectionStates.currentAddress(nodeConnectionId);
            log.debug("Initiating connection to node {} using address {}", node, address);
            // 使用网络模块开启连接 同样由于是选择器模式 本线程只会尝试性连接 失败后等待io线程监听选择器
            selector.connect(nodeConnectionId,
                    new InetSocketAddress(address, node.port()),
                    this.socketSendBuffer,
                    this.socketReceiveBuffer);
        } catch (IOException e) {
            log.warn("Error connecting to node {}", node, e);
            // Attempt failed, we'll try again after the backoff
            connectionStates.disconnected(nodeConnectionId, now);
            // Notify metadata updater of the connection failure
            metadataUpdater.handleServerDisconnect(now, nodeConnectionId, Optional.empty());
        }
    }

    /**
     * 默认的元数据更新对象
     */
    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        // Defined if there is a request in progress, null otherwise
        // 该值被设置就代表已经发出了一个更新请求，并且还未处理完毕
        private InProgressData inProgress;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.inProgress = null;
        }

        /**
         * 从元数据中获取集群中存在的所有节点
         * @return
         */
        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        /**
         * 判断是否即将更新元数据
         */
        @Override
        public boolean isUpdateDue(long now) {
            // 为false的情况下判断是否已经到了获取元数据的时间
            return !hasFetchInProgress() && this.metadata.timeToNextUpdate(now) == 0;
        }

        private boolean hasFetchInProgress() {
            return inProgress != null;
        }

        /**
         * 如果此时元数据还有效 返回元数据的超时时间 如果无效，返回更新元数据需要的时间
         * @param now
         * @return
         */
        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            // 这里在计算多久后元数据超时
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            // 如果此时已经发起了一个请求 至少要等待它超时
            long waitForMetadataFetch = hasFetchInProgress() ? defaultRequestTimeoutMs : 0;

            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {
                return metadataTimeout;
            }

            // 代表元数据已经超时  立即发起一次更新请求
            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            // 这里会找到一个负载最小的节点 此时可能返回的是一个还未连接的节点(或者连接中的)
            Node node = leastLoadedNode(now);
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                // 此时没有任何一个节点可用 要等待一个补偿时间
                return reconnectBackoffMs;
            }

            // 往该node发送获取元数据信息的请求
            return maybeUpdate(now, node);
        }

        /**
         * 当与某个服务器断开连接时调用该方法
         * @param now Current time in milliseconds
         * @param destinationId
         * @param maybeFatalException
         */
        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            // 从缓存中获取此时的集群信息
            Cluster cluster = metadata.fetch();
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where a connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            // TODO 当该标识为true时 获取node信息
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destinationId);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            // If we have a disconnect while an update is due, we treat it as a failed update
            // so that we can backoff properly
            // 此时正在更新元数据 那么可以认为本次元数据信息存在问题
            if (isUpdateDue(now))
                handleFailedRequest(now, Optional.empty());

            // 如果出现了fatalError 设置到metadata上
            maybeFatalException.ifPresent(metadata::fatalError);

            // The disconnect may be the result of stale metadata, so request an update
            // 此时node信息可能过期了 标记成需要全量更新元数据
            metadata.requestUpdate();
        }

        /**
         * 代表本次拉取到一个错误的元数据信息
         * @param now Current time in milliseconds
         * @param maybeFatalException Optional fatal error (e.g. {@link UnsupportedVersionException})
         */
        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            maybeFatalException.ifPresent(metadata::fatalError);
            // 记录失败时间以及清空对象
            metadata.failedUpdate(now);
            inProgress = null;
        }

        /**
         * 处理返回的有效的元数据信息
         * @param requestHeader
         * @param now
         * @param response
         */
        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            // If any partition has leader with missing listeners, log up to ten of these partitions
            // for diagnosing broker configuration issues.
            // This could be a transient issue if listeners were added dynamically to brokers.
            // 读取所有有关topic以及分区的信息
            List<TopicPartition> missingListenerPartitions = response.topicMetadata().stream().flatMap(topicMetadata ->
                    topicMetadata.partitionMetadata().stream()
                            .filter(partitionMetadata -> partitionMetadata.error == Errors.LISTENER_NOT_FOUND)
                            .map(partitionMetadata -> new TopicPartition(topicMetadata.topic(), partitionMetadata.partition())))
                    .collect(Collectors.toList());
            if (!missingListenerPartitions.isEmpty()) {
                int count = missingListenerPartitions.size();
                log.warn("{} partitions have leader brokers without a matching listener, including {}",
                        count, missingListenerPartitions.subList(0, Math.min(10, count)));
            }

            // Check if any topic's metadata failed to get updated
            // 检查是否返回了异常信息
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", requestHeader.correlationId(), errors);

            // Don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            // 如果brokers为空 认为本次结果是有问题的
            if (response.brokers().isEmpty()) {
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now);
            } else {
                // 更新本地元数据信息
                this.metadata.update(inProgress.requestVersion, response, inProgress.isPartialUpdate, now);
            }

            inProgress = null;
        }

        @Override
        public void close() {
            this.metadata.close();
        }

        /**
         * Return true if there's at least one connection establishment is currently underway
         */
        private boolean isAnyNodeConnecting() {
            for (Node node : fetchNodes()) {
                if (connectionStates.isConnecting(node.idString())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         * @param node 往该节点发送获取元数据的请求
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            // 判断能否往该节点发送请求  如果连接还未建立 返回false
            if (canSendRequest(nodeConnectionId, now)) {
                // 构建请求体对象
                Metadata.MetadataRequestAndVersion requestAndVersion = metadata.newMetadataRequestAndVersion(now);
                MetadataRequest.Builder metadataRequest = requestAndVersion.requestBuilder;
                log.debug("Sending metadata request {} to node {}", metadataRequest, node);

                // 发送请求 由业务线程设置到网络模块上 由io线程异步写出
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                // 初始化inProgress对象 代表此时已经发起了一个更新元数据请求
                inProgress = new InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate);
                return defaultRequestTimeoutMs;
            }

            // 此时可能没有可以直接使用的node连接 比如囤积的请求过多或者连接未建立

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            // 检测此时是否有正在建立的连接 有的话直接返回一个等待时间
            if (isAnyNodeConnecting()) {
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                return reconnectBackoffMs;
            }

            // 代表此时还没有开始连接的node  这里尝试创建连接
            if (connectionStates.canConnect(nodeConnectionId, now)) {
                // We don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node);
                // 开始建立连接  并返回一个重连补偿时间
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            // 此时没有满足连接条件的节点 时间不可控 返回一个特殊值
            return Long.MAX_VALUE;
        }

        /**
         * 此时正在发起的某个更新元数据的请求
         */
        public class InProgressData {
            /**
             * 请求的版本号
             */
            public final int requestVersion;
            /**
             * 是增量更新还是全量更新
             */
            public final boolean isPartialUpdate;

            private InProgressData(int requestVersion, boolean isPartialUpdate) {
                this.requestVersion = requestVersion;
                this.isPartialUpdate = isPartialUpdate;
            }
        }

    }

    /**
     * 基于当前信息构建通用请求体对象
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response   是否会返回结果对象
     * @return
     */
    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
    }

    // visible for testing
    int nextCorrelationId() {
        if (SaslClientAuthenticator.isReserved(correlation)) {
            // the numeric overflow is fine as negative values is acceptable
            correlation = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + 1;
        }
        return correlation++;
    }

    /**
     * 基于当前信息构建通用请求体对象
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     * @param requestTimeoutMs Upper bound time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may get cancelled sooner if the socket disconnects
     *                         for any reason including if another pending request to the same node timed out first.
     * @param callback the callback to invoke when we get a response   该属性可能为null
     * @return
     */
    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, nextCorrelationId(), clientId, createdTimeMs, expectResponse,
                requestTimeoutMs, callback);
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }

    /**
     * 一个简单的bean对象
     */
    static class InFlightRequest {
        final RequestHeader header;
        final String destination;
        final RequestCompletionHandler callback;
        final boolean expectResponse;
        final AbstractRequest request;
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;
        final long createdTimeMs;
        final long requestTimeoutMs;

        public InFlightRequest(ClientRequest clientRequest,
                               RequestHeader header,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this(header,
                    clientRequest.requestTimeoutMs(),
                    clientRequest.createdTimeMs(),
                    clientRequest.destination(),
                    clientRequest.callback(),
                    clientRequest.expectResponse(),
                    isInternalRequest,
                    request,
                    send,
                    sendTimeMs);
        }

        public InFlightRequest(RequestHeader header,
                               int requestTimeoutMs,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.requestTimeoutMs = requestTimeoutMs;
            this.createdTimeMs = createdTimeMs;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
        }

        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    false, null, null, response);
        }

        public ClientResponse disconnected(long timeMs, AuthenticationException authenticationException) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, null, authenticationException, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }
    }

}
