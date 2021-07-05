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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.Closeable;
import java.util.List;

/**
 * The interface for {@link NetworkClient
 * kafka客户端抽象层
 */
public interface KafkaClient extends Closeable {

    /**
     * Check if we are currently ready to send another request to the given node but don't attempt to connect if we
     * aren't.
     *
     * @param node The node to check
     * @param now The current timestamp
     *            判断与某个节点的连接是否建立
     */
    boolean isReady(Node node, long now);

    /**
     * Initiate a connection to the given node (if necessary), and return true if already connected. The readiness of a
     * node will change only when poll is invoked.
     *
     * @param node The node to connect to.
     * @param now The current time
     * @return true iff we are ready to immediately initiate the sending of another request to the given node.
     * 开始与指定的节点建立连接
     */
    boolean ready(Node node, long now);

    /**
     * Return the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     * 基于与该节点当前的连接状态返回一个延迟时间 比如发现此时连接断开 返回的就是重连时间间隔
     */
    long connectionDelay(Node node, long now);

    /**
     * Return the number of milliseconds to wait, based on the connection state and the throttle time, before
     * attempting to send data. If the connection has been established but being throttled, return throttle delay.
     * Otherwise, return connection delay.
     *
     * @param node the connection to check
     * @param now the current time in ms
     */
    long pollDelayMs(Node node, long now);

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     * 判断与某个节点的连接是否失败了
     */
    boolean connectionFailed(Node node);

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     * 当与该节点连接失败时检测是否是认证异常
     */
    AuthenticationException authenticationException(Node node);

    /**
     * Queue up the given request for sending. Requests can only be sent on ready connections.
     * @param request The request
     * @param now The current timestamp
     *            将某个请求对象发送到目标节点 clientRequest中携带了目的地信息
     */
    void send(ClientRequest request, long now);

    /**
     * Do actual reads and writes from sockets.
     *
     * @param timeout The maximum amount of time to wait for responses in ms, must be non-negative. The implementation
     *                is free to use a lower value if appropriate (common reasons for this are a lower request or
     *                metadata update timeout)
     * @param now The current time in ms
     * @throws IllegalStateException If a request is sent to an unready node
     * 从某个节点拉取数据 等待一定时间并返回一组响应对象
     */
    List<ClientResponse> poll(long timeout, long now);

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     *               与某个节点断开连接
     */
    void disconnect(String nodeId);

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     *               关闭某个节点
     */
    void close(String nodeId);

    /**
     * Choose the node with the fewest outstanding requests. This method will prefer a node with an existing connection,
     * but will potentially choose a node for which we don't yet have a connection if all existing connections are in
     * use.
     *
     * @param now The current time in ms
     * @return The node with the fewest in-flight requests.
     * 选择一个负载最低的节点
     */
    Node leastLoadedNode(long now);

    /**
     * The number of currently in-flight requests for which we have not yet returned a response
     * 有多少请求还没有收到结果
     */
    int inFlightRequestCount();

    /**
     * Return true if there is at least one in-flight request and false otherwise.
     * 是否有请求还未收到结果
     */
    boolean hasInFlightRequests();

    /**
     * Get the total in-flight requests for a particular node
     *
     * @param nodeId The id of the node
     *               针对某个节点判断是否有未收到响应的请求
     */
    int inFlightRequestCount(String nodeId);

    /**
     * Return true if there is at least one in-flight request for a particular node and false otherwise.
     */
    boolean hasInFlightRequests(String nodeId);

    /**
     * Return true if there is at least one node with connection in the READY state and not throttled. Returns false
     * otherwise.
     *
     * @param now the current time
     *            判断是否至少有一个已经建立连接的节点
     */
    boolean hasReadyNodes(long now);

    /**
     * Wake up the client if it is currently blocked waiting for I/O
     * 唤醒时间循转
     */
    void wakeup();

    /**
     * Create a new ClientRequest.
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     *                       构建一个请求对象
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs, boolean expectResponse);

    /**
     * Create a new ClientRequest.
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     * @param requestTimeoutMs Upper bound time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may get cancelled sooner if the socket disconnects
     *                         for any reason including if another pending request to the same node timed out first.
     * @param callback the callback to invoke when we get a response
     */
    ClientRequest newClientRequest(String nodeId,
                                   AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs,
                                   boolean expectResponse,
                                   int requestTimeoutMs,
                                   RequestCompletionHandler callback);



    /**
     * Initiates shutdown of this client. This method may be invoked from another thread while this
     * client is being polled. No further requests may be sent using the client. The current poll()
     * will be terminated using wakeup(). The client should be explicitly shutdown using {@link #close()}
     * after poll returns. Note that {@link #close()} should not be invoked concurrently while polling.
     * 关闭当前客户端
     */
    void initiateClose();

    /**
     * Returns true if the client is still active. Returns false if {@link #initiateClose()} or {@link #close()}
     * was invoked for this client.
     * 判断client是否处于激活状态
     */
    boolean active();

}
