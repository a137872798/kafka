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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This class manages the coordination process with the consumer coordinator.
 * 消费者协调对象
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    private final GroupRebalanceConfig rebalanceConfig;
    private final Logger log;
    private final List<ConsumerPartitionAssignor> assignors;
    private final ConsumerMetadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    /**
     * 默认的提交偏移量处理回调
     */
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    private final boolean autoCommitEnabled;
    private final int autoCommitIntervalMs;
    private final ConsumerInterceptors<?, ?> interceptors;
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    // 有关提交偏移量的回调对象会存储在这个链表中
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;

    /**
     * 在同一个group下的所有consumer有一个leader的概念  由leader来进行真正的分配工作
     */
    private boolean isLeader = false;
    /**
     * 最近一次发起join请求时订阅的tp 可能是pattern模式
     */
    private Set<String> joinedSubscription;
    private MetadataSnapshot metadataSnapshot;
    /**
     * 最近一次产生分配结果时 对应的快照
     */
    private MetadataSnapshot assignmentSnapshot;
    /**
     * 维护有关自动提交偏移量的时间
     */
    private Timer nextAutoCommitTimer;
    /**
     * 本节点在提交偏移量时 被强制拦下来了
     */
    private AtomicBoolean asyncCommitFenced;

    /**
     * 每当join到一个group中就会存储相关信息 每次变化对应着group内成员的变化 add/remove
     */
    private ConsumerGroupMetadata groupMetadata;
    private final boolean throwOnFetchStableOffsetsUnsupported;

    // hold onto request&future for committed offset requests to enable async calls.
    private PendingCommittedOffsetRequest pendingCommittedOffsetRequest = null;

    private static class PendingCommittedOffsetRequest {
        private final Set<TopicPartition> requestedPartitions;
        private final Generation requestedGeneration;
        private final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response;

        private PendingCommittedOffsetRequest(final Set<TopicPartition> requestedPartitions,
                                              final Generation generationAtRequestTime,
                                              final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> response) {
            this.requestedPartitions = Objects.requireNonNull(requestedPartitions);
            this.response = Objects.requireNonNull(response);
            this.requestedGeneration = generationAtRequestTime;
        }

        private boolean sameRequest(final Set<TopicPartition> currentRequest, final Generation currentGeneration) {
            return Objects.equals(requestedGeneration, currentGeneration) && requestedPartitions.equals(currentRequest);
        }
    }

    private final RebalanceProtocol protocol;

    /**
     * Initialize the coordination manager.
     * 在初始化consumer时 如果设置了groupId 就会初始化这个协调对象
     */
    public ConsumerCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               List<ConsumerPartitionAssignor> assignors,
                               ConsumerMetadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean throwOnFetchStableOffsetsUnsupported) {
        super(rebalanceConfig,
              logContext,
              client,
              metrics,
              metricGrpPrefix,
              time);
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        // 基于当前元数据生成快照信息 在初始阶段 metadata中可能只有 bootstrapServer的地址
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch(), metadata.updateVersion());
        this.subscriptions = subscriptions;
        // 这里是默认的提交偏移量时的回调对象
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        // 此时消费者是否开启了自动提交消费偏移量
        this.autoCommitEnabled = autoCommitEnabled;
        // 为了提高效率 并不会在每次处理完消息后都提交偏移量 而是每隔一顿时间提交偏移量
        this.autoCommitIntervalMs = autoCommitIntervalMs;

        this.assignors = assignors;

        // 该对象存储提交偏移量后的callback对象
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        // TODO
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        // 在这里主要是触发拦截器的 onCommit
        this.interceptors = interceptors;
        // 此时待发送的提交请求
        this.pendingAsyncCommits = new AtomicInteger();
        // 当本节点提交偏移量时返回本实例被禁止时 会修改该标记
        this.asyncCommitFenced = new AtomicBoolean(false);

        // 此时为消费者组指定了groupId后 就会有有关该组的元数据信息
        this.groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID, JoinGroupRequest.UNKNOWN_MEMBER_ID, rebalanceConfig.groupInstanceId);
        // 默认false
        this.throwOnFetchStableOffsetsUnsupported = throwOnFetchStableOffsetsUnsupported;

        // 计算下一次提交偏移量的时间戳
        if (autoCommitEnabled)
            this.nextAutoCommitTimer = time.timer(autoCommitIntervalMs);

        // select the rebalance protocol such that:
        //   1. only consider protocols that are supported by all the assignors. If there is no common protocols supported
        //      across all the assignors, throw an exception.
        //   2. if there are multiple protocols that are commonly supported, select the one with the highest id (i.e. the
        //      id number indicates how advanced the protocol is).
        // we know there are at least one assignor in the list, no need to double check for NPE
        // 当此时指定了分配分区的assignors对象
        if (!assignors.isEmpty()) {

            // 这里求这些分配对象支持的协议交集 如果交集为空 代表不支持任何协议
            List<RebalanceProtocol> supportedProtocols = new ArrayList<>(assignors.get(0).supportedProtocols());

            for (ConsumerPartitionAssignor assignor : assignors) {
                supportedProtocols.retainAll(assignor.supportedProtocols());
            }

            if (supportedProtocols.isEmpty()) {
                throw new IllegalArgumentException("Specified assignors " +
                    assignors.stream().map(ConsumerPartitionAssignor::name).collect(Collectors.toSet()) +
                    " do not have commonly supported rebalance protocol");
            }

            Collections.sort(supportedProtocols);

            // 使用最后一个共同支持的协议
            protocol = supportedProtocols.get(supportedProtocols.size() - 1);
        } else {
            protocol = null;
        }

        // 由于刚初始化 还没有任何元数据信息 此时可以开始更新元数据了
        this.metadata.requestUpdate();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    /**
     * 在发送join请求前 就要获取元数据信息
     * @return
     */
    @Override
    protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
        log.debug("Joining group with current subscription: {}", subscriptions.subscription());
        // 记录最近一次join时的topic 当订阅的topic发生变化要重新发送join
        // 只有当coordinator知道某个group下consumer订阅的所有topic才知道怎么分配合适 因为每个consumer订阅的topic可能不同
        this.joinedSubscription = subscriptions.subscription();
        JoinGroupRequestData.JoinGroupRequestProtocolCollection protocolSet = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();

        List<String> topics = new ArrayList<>(joinedSubscription);
        // 在本地先使用assignor选择分区
        for (ConsumerPartitionAssignor assignor : assignors) {
            Subscription subscription = new Subscription(topics,
                                                         // 附带一些用户自定义的数据
                                                         assignor.subscriptionUserData(joinedSubscription),
                                                         // 将本节点之前分配到的tp设置进去 在prepareJoin阶段可能会清理掉这些数据 取决于protocol
                                                         subscriptions.assignedPartitionsList());
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);

            protocolSet.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(assignor.name())
                    .setMetadata(Utils.toArray(metadata)));
        }
        return protocolSet;
    }

    /**
     * @param cluster
     */
    public void updatePatternSubscription(Cluster cluster) {
        // 基于当前cluster快照 找到所有满足正则的topic
        final Set<String> topicsToSubscribe = cluster.topics().stream()
                .filter(subscriptions::matchesSubscribedPattern)
                .collect(Collectors.toSet());
        // 更新此时订阅的所有topic
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics();
    }

    private ConsumerPartitionAssignor lookupAssignor(String name) {
        for (ConsumerPartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    /**
     * 可能增加了订阅的topic
     * @param assignedPartitions
     */
    private void maybeUpdateJoinedSubscription(Set<TopicPartition> assignedPartitions) {
        if (subscriptions.hasPatternSubscription()) {
            // Check if the assignment contains some topics that were not in the original
            // subscription, if yes we will obey what leader has decided and add these topics
            // into the subscriptions as long as they still match the subscribed pattern

            Set<String> addedTopics = new HashSet<>();
            // this is a copy because its handed to listener below
            for (TopicPartition tp : assignedPartitions) {
                if (!joinedSubscription.contains(tp.topic()))
                    addedTopics.add(tp.topic());
            }

            // 代表新增了topic
            if (!addedTopics.isEmpty()) {
                Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
                Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
                newSubscription.addAll(addedTopics);
                newJoinedSubscription.addAll(addedTopics);

                // 代表本节点之前没有观测到某个topic 需要更新元数据
                if (this.subscriptions.subscribeFromPattern(newSubscription))
                    metadata.requestUpdateForNewTopics();
                this.joinedSubscription = newJoinedSubscription;
            }
        }
    }

    /**
     * 使用用户指定的assignor 来处理本次分配到的结果 默认为NOOP
     * @param assignor
     * @param assignment
     * @return
     */
    private Exception invokeOnAssignment(final ConsumerPartitionAssignor assignor, final Assignment assignment) {
        log.info("Notifying assignor about the new {}", assignment);

        try {
            assignor.onAssignment(assignment, groupMetadata);
        } catch (Exception e) {
            return e;
        }

        return null;
    }

    /**
     * 触发rebalance监听器  检测是否符合条件
     * @param assignedPartitions
     * @return
     */
    private Exception invokePartitionsAssigned(final Set<TopicPartition> assignedPartitions) {
        log.info("Adding newly assigned partitions: {}", Utils.join(assignedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsAssigned(assignedPartitions);
            sensors.assignCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsAssigned for partitions {}",
                listener.getClass().getName(), assignedPartitions, e);
            return e;
        }

        return null;
    }

    private Exception invokePartitionsRevoked(final Set<TopicPartition> revokedPartitions) {
        log.info("Revoke previously assigned partitions {}", Utils.join(revokedPartitions, ", "));

        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsRevoked(revokedPartitions);
            sensors.revokeCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsRevoked for partitions {}",
                listener.getClass().getName(), revokedPartitions, e);
            return e;
        }

        return null;
    }

    /**
     * 由于gen被重置 放弃之前缓存的tp信息
     * @param lostPartitions
     * @return
     */
    private Exception invokePartitionsLost(final Set<TopicPartition> lostPartitions) {
        log.info("Lost previously assigned partitions {}", Utils.join(lostPartitions, ", "));

        // 这个监听器是用户设置的钩子
        ConsumerRebalanceListener listener = subscriptions.rebalanceListener();
        try {
            final long startMs = time.milliseconds();
            listener.onPartitionsLost(lostPartitions);
            sensors.loseCallbackSensor.record(time.milliseconds() - startMs);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on invocation of onPartitionsLost for partitions {}",
                listener.getClass().getName(), lostPartitions, e);
            return e;
        }

        return null;
    }

    /**
     * 处理收到的分配结果
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param assignmentStrategy
     * @param assignmentBuffer
     */
    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        log.debug("Executing onJoinComplete with generation {} and memberId {}", generation, memberId);

        // Only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        // follower不需要维护这个信息
        if (!isLeader)
            assignmentSnapshot = null;

        // 确保本次leader采用的分配策略确实存在于member
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // Give the assignor a chance to update internal state based on the received assignment
        // 更新当前group信息
        groupMetadata = new ConsumerGroupMetadata(rebalanceConfig.groupId, generation, memberId, rebalanceConfig.groupInstanceId);

        // 只看eager模式的话 本节点是不会保留tp的
        Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        // should at least encode the short version
        if (assignmentBuffer.remaining() < 2)
            throw new IllegalStateException("There are insufficient bytes available to read assignment from the sync-group response (" +
                "actual byte size " + assignmentBuffer.remaining() + ") , this is not expected; " +
                "it is possible that the leader's assign function is buggy and did not return any assignment for this member, " +
                "or because static member is configured and the protocol is buggy hence did not get the assignment for this member");

        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        Set<TopicPartition> assignedPartitions = new HashSet<>(assignment.partitions());

        // 确保返回的tp确实被订阅
        if (!subscriptions.checkAssignmentMatchedSubscription(assignedPartitions)) {
            log.warn("We received an assignment {} that doesn't match our current subscription {}; it is likely " +
                "that the subscription has changed since we joined the group. Will try re-join the group with current subscription",
                assignment.partitions(), subscriptions.prettyString());

            // 在下一轮中重新发起join请求
            requestRejoin();

            return;
        }

        final AtomicReference<Exception> firstException = new AtomicReference<>(null);
        Set<TopicPartition> addedPartitions = new HashSet<>(assignedPartitions);
        addedPartitions.removeAll(ownedPartitions);

        // TODO
        if (protocol == RebalanceProtocol.COOPERATIVE) {
            Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);

            log.info("Updating assignment with\n" +
                    "\tAssigned partitions:                       {}\n" +
                    "\tCurrent owned partitions:                  {}\n" +
                    "\tAdded partitions (assigned - owned):       {}\n" +
                    "\tRevoked partitions (owned - assigned):     {}\n",
                assignedPartitions,
                ownedPartitions,
                addedPartitions,
                revokedPartitions
            );

            if (!revokedPartitions.isEmpty()) {
                // Revoke partitions that were previously owned but no longer assigned;
                // note that we should only change the assignment (or update the assignor's state)
                // AFTER we've triggered  the revoke callback
                firstException.compareAndSet(null, invokePartitionsRevoked(revokedPartitions));

                // If revoked any partitions, need to re-join the group afterwards
                log.info("Need to revoke partitions {} and re-join the group", revokedPartitions);
                requestRejoin();
            }
        }

        // The leader may have assigned partitions which match our subscription pattern, but which
        // were not explicitly requested, so we update the joined subscription here.
        // 如果是pattern模式 可能新增了topic 需要更新订阅数据
        maybeUpdateJoinedSubscription(assignedPartitions);

        // Catch any exception here to make sure we could complete the user callback.
        // 使用用户钩子检测分配结果
        firstException.compareAndSet(null, invokeOnAssignment(assignor, assignment));

        // Reschedule the auto commit starting from now
        // 重启自动提交
        if (autoCommitEnabled)
            this.nextAutoCommitTimer.updateAndReset(autoCommitIntervalMs);

        // 更新本节点分配到的所有tp
        subscriptions.assignFromSubscribed(assignedPartitions);

        // Add partitions that were not previously owned but are now assigned
        firstException.compareAndSet(null, invokePartitionsAssigned(addedPartitions));

        if (firstException.get() != null) {
            if (firstException.get() instanceof KafkaException) {
                throw (KafkaException) firstException.get();
            } else {
                throw new KafkaException("User rebalance callback throws an error", firstException.get());
            }
        }
    }

    /**
     * 检测是否需要更新元数据快照
     */
    void maybeUpdateSubscriptionMetadata() {
        int version = metadata.updateVersion();
        // 当前版本超过了快照版本
        if (version > metadataSnapshot.version) {
            Cluster cluster = metadata.fetch();

            // 基于pattern 更新订阅的topic
            if (subscriptions.hasPatternSubscription())
                updatePatternSubscription(cluster);

            // Update the current snapshot, which will be used to check for subscription
            // changes that would require a rebalance (e.g. new partitions).
            // 基于最新的订阅状态描述对象 生成新的快照对象
            metadataSnapshot = new MetadataSnapshot(subscriptions, cluster, version);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     * <p>
     * Returns early if the timeout expires or if waiting on rejoin is not required
     *
     * @param timer Timer bounding how long this method can block
     * @param waitForJoinGroup Boolean flag indicating if we should wait until re-join group completes   本次调用是否需要阻塞直到加入到group中
     * @throws KafkaException if the rebalance callback throws an exception
     * @return true iff the operation succeeded
     */
    public boolean poll(Timer timer, boolean waitForJoinGroup) {
        // 尝试更新元数据快照
        maybeUpdateSubscriptionMetadata();

        // 这里找到所有有关提交偏移量的callback对象并执行
        invokeCompletedOffsetCommitCallbacks();

        // 当消费者订阅的是topic或者pattern都是进入这个分支
        if (subscriptions.hasAutoAssignedPartitions()) {
            // 在初始化该对象的时候 根据指定的ConsumerPartitionAssignor交集 得到唯一的协议对象
            if (protocol == null) {
                throw new IllegalStateException("User configured " + ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG +
                    " to empty while trying to subscribe for group protocol to auto assign partitions");
            }
            // Always update the heartbeat last poll time so that the heartbeat thread does not leave the
            // group proactively due to application inactivity even if (say) the coordinator cannot be found.
            // 发起心跳检测
            pollHeartbeat(timer.currentTimeMs());
            // 协调者需要确保目的node存在 才能知道去哪里协调
            // 允许等待一定的时间 并判断在等待一定时间后能否连接上node
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            // 通过检测发现元数据发生了变化 或者订阅信息发生了变化 就会需要发起一个rejoin请求 (可能已经发出请求 但是没有收到结果也会进入这个分支)
            if (rejoinNeededOrPending()) {
                // due to a race condition between the initial metadata fetch and the initial rebalance,
                // we need to ensure that the metadata is fresh before joining initially. This ensures
                // that we have matched the pattern against the cluster's topics at least once before joining.
                // 本次是pattern的订阅模式
                if (subscriptions.hasPatternSubscription()) {
                    // For consumer group that uses pattern-based subscription, after a topic is created,
                    // any consumer that discovers the topic after metadata refresh can trigger rebalance
                    // across the entire consumer group. Multiple rebalances can be triggered after one topic
                    // creation if consumers refresh metadata at vastly different times. We can significantly
                    // reduce the number of rebalances caused by single topic creation by asking consumer to
                    // refresh metadata before re-joining the group as long as the refresh backoff time has
                    // passed.
                    // 代表到了更新元数据的时候了 设置需要更新的标记
                    if (this.metadata.timeToAllowUpdate(timer.currentTimeMs()) == 0) {
                        this.metadata.requestUpdate();
                    }

                    // 代表此时元数据还未更新完成(给予的时间不够) 只能返回false等待下次调用
                    if (!client.ensureFreshMetadata(timer)) {
                        return false;
                    }

                    // 基于最新的元数据更新 pattern能够对应到的tp
                    maybeUpdateSubscriptionMetadata();
                }

                // if not wait for join group, we would just use a timer of 0
                // ensureActiveGroup 会确保consumer加入到group 并获得分配结果
                // waitForJoin为false的情况 最终时间为0
                if (!ensureActiveGroup(waitForJoinGroup ? timer : time.timer(0L))) {
                    // since we may use a different timer in the callee, we'd still need
                    // to update the original timer's current time after the call
                    timer.update(time.milliseconds());

                    // 在指定时间内没有获取到分配信息
                    return false;
                }
            }
        } else {
            // For manually assigned partitions, if there are no ready nodes, await metadata.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls. Without
            // the wakeup, poll() with no channels would block for the timeout, delaying re-connection.
            // awaitMetadataUpdate() initiates new connections with configured backoff and avoids the busy loop.
            // When group management is used, metadata wait is already performed for this scenario as
            // coordinator is unknown, hence this check is not required.
            // TODO 非auto订阅模式 先放置
            if (metadata.updateRequested() && !client.hasReadyNodes(timer.currentTimeMs())) {
                client.awaitMetadataUpdate(timer);
            }
        }

        // 每次都会检测是否需要提交最新的偏移量
        maybeAutoCommitOffsetsAsync(timer.currentTimeMs());
        return true;
    }

    /**
     * Return the time to the next needed invocation of {@link ConsumerNetworkClient#poll(Timer)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        return Math.min(nextAutoCommitTimer.remainingMs(), timeToNextHeartbeat(now));
    }

    /**
     * 作为leader节点 更新groupTopic
     * @param topics
     */
    private void updateGroupSubscription(Set<String> topics) {
        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        // 代表本节点还没有获取到所有相关topic的信息 需要先更新信息  需要知道新的topic下有多少分区
        if (this.subscriptions.groupSubscribe(topics))
            metadata.requestUpdateForNewTopics();

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        // 确保有最新的元数据
        if (!client.ensureFreshMetadata(time.timer(Long.MAX_VALUE)))
            throw new TimeoutException();

        // 更新subscriptions信息
        maybeUpdateSubscriptionMetadata();
    }

    /**
     * 作为leader中的group节点 具备分配权 再分配后会将结果同步到coordinator，之后再由coordinator通知到其他member 这样同一group下的consumer不需要相互感知
     * 难怪叫协调者 协调者负责感应member的上下线，而consumer.group.leader真正进行分配
     * @param leaderId The id of the leader (which is this member)
     * @param assignmentStrategy  coordinator在获取到group下consumer支持的所有分配策略后会选择唯一一个交集
     * @param allSubscriptions 存储group下所有member的信息
     * @return
     */
    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        List<JoinGroupResponseData.JoinGroupResponseMember> allSubscriptions) {
        // assignmentStrategy 应该是取group下所有member的分配策略交集 只会返回一个
        ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // 存储group下出现的所有topic
        Set<String> allSubscribedTopics = new HashSet<>();

        // 按照member对所有member分组
        Map<String, Subscription> subscriptions = new HashMap<>();

        // collect all the owned partitions
        // 有个策略会保留之前已经分配好的tp
        Map<String, List<TopicPartition>> ownedPartitions = new HashMap<>();

        // 遍历每个member对象
        for (JoinGroupResponseData.JoinGroupResponseMember memberSubscription : allSubscriptions) {
            // 解码后还原订阅信息 描述了该member订阅的所有topic
            Subscription subscription = ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(memberSubscription.metadata()));
            subscription.setGroupInstanceId(Optional.ofNullable(memberSubscription.groupInstanceId()));
            subscriptions.put(memberSubscription.memberId(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
            ownedPartitions.put(memberSubscription.memberId(), subscription.ownedPartitions());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        // 可能本节点没有维护某些新的topic信息 就需要更新元数据 这样知道最新的partition信息就便于更精确的分配了
        updateGroupSubscription(allSubscribedTopics);

        // 收到onLeader请求 代表本consumer作为leader节点
        isLeader = true;

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignor.name(), subscriptions);

        // 这里已经产生了分配结果 key是memberId value是分配到该成员下的所有tp
        Map<String, Assignment> assignments = assignor.assign(metadata.fetch(), new GroupSubscription(subscriptions)).groupAssignment();

        // 一般的协议都是使用 eager 也就是完全由leader决定 这里是校验性逻辑先忽略
        if (protocol == RebalanceProtocol.COOPERATIVE) {
            validateCooperativeAssignment(ownedPartitions, assignments);
        }

        // user-customized assignor may have created some topics that are not in the subscription list
        // and assign their partitions to the members; in this case we would like to update the leader's
        // own metadata with the newly added topics so that it will not trigger a subsequent rebalance
        // when these topics gets updated from metadata refresh.
        //
        // TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
        //       we may need to modify the ConsumerPartitionAssignor API to better support this case.
        // 找到实际有效的topic 某些topic可能还没有分区信息 或者还不存在 就无法参与分配
        Set<String> assignedTopics = new HashSet<>();
        for (Assignment assigned : assignments.values()) {
            for (TopicPartition tp : assigned.partitions())
                assignedTopics.add(tp.topic());
        }

        // 仅打印日志
        if (!assignedTopics.containsAll(allSubscribedTopics)) {
            Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
            notAssignedTopics.removeAll(assignedTopics);
            log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
        }

        // 由于assignor是由用户自己实现的 可能会新增topic 这里将新增的补上
        if (!allSubscribedTopics.containsAll(assignedTopics)) {
            Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
            newlyAddedTopics.removeAll(allSubscribedTopics);
            log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

            allSubscribedTopics.addAll(assignedTopics);
            updateGroupSubscription(allSubscribedTopics);
        }

        // 记录最新一次产生分配结果时对应的快照信息
        assignmentSnapshot = metadataSnapshot;

        log.info("Finished assignment for group at generation {}: {}", generation().generationId, assignments);

        // 将分配结果转换成数据包后 发送到coordinator 之后coordinator会通知到group下的其他member
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignments.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    /**
     * Used by COOPERATIVE rebalance protocol only.
     *
     * Validate the assignments returned by the assignor such that no owned partitions are going to
     * be reassigned to a different consumer directly: if the assignor wants to reassign an owned partition,
     * it must first remove it from the new assignment of the current owner so that it is not assigned to any
     * member, and then in the next rebalance it can finally reassign those partitions not owned by anyone to consumers.
     */
    private void validateCooperativeAssignment(final Map<String, List<TopicPartition>> ownedPartitions,
                                               final Map<String, Assignment> assignments) {
        Set<TopicPartition> totalRevokedPartitions = new HashSet<>();
        Set<TopicPartition> totalAddedPartitions = new HashSet<>();
        for (final Map.Entry<String, Assignment> entry : assignments.entrySet()) {
            final Assignment assignment = entry.getValue();
            final Set<TopicPartition> addedPartitions = new HashSet<>(assignment.partitions());
            addedPartitions.removeAll(ownedPartitions.get(entry.getKey()));
            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions.get(entry.getKey()));
            revokedPartitions.removeAll(assignment.partitions());

            totalAddedPartitions.addAll(addedPartitions);
            totalRevokedPartitions.addAll(revokedPartitions);
        }

        // if there are overlap between revoked partitions and added partitions, it means some partitions
        // immediately gets re-assigned to another member while it is still claimed by some member
        totalAddedPartitions.retainAll(totalRevokedPartitions);
        if (!totalAddedPartitions.isEmpty()) {
            log.error("With the COOPERATIVE protocol, owned partitions cannot be " +
                "reassigned to other members; however the assignor has reassigned partitions {} which are still owned " +
                "by some members", totalAddedPartitions);

            throw new IllegalStateException("Assignor supporting the COOPERATIVE protocol violates its requirements");
        }
    }

    /**
     * 为join做一些准备工作 传入的2个参数是 有关上一次group信息的
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        log.debug("Executing onJoinPrepare with generation {} and memberId {}", generation, memberId);
        // commit offsets prior to rebalance if auto-commit enabled
        // 如果设置了自动提交偏移量 那么在rebalance之前会先提交偏移量
        // 在下面会清理tp信息 那么之后自然就无法自动提交偏移量了 节省了无意义的开销
        maybeAutoCommitOffsetsSync(time.timer(rebalanceConfig.rebalanceTimeoutMs));

        // the generation / member-id can possibly be reset by the heartbeat thread
        // upon getting errors or heartbeat timeouts; in this case whatever is previously
        // owned partitions would be lost, we should trigger the callback and cleanup the assignment;
        // otherwise we can proceed normally and revoke the partitions depending on the protocol,
        // and in that case we should only change the assignment AFTER the revoke callback is triggered
        // so that users can still access the previously owned partitions to commit offsets etc.
        Exception exception = null;
        final Set<TopicPartition> revokedPartitions;

        // 代表还没有加入到group中 或者gen进行了重置
        if (generation == Generation.NO_GENERATION.generationId &&
            memberId.equals(Generation.NO_GENERATION.memberId)) {
            revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());

            // 代表generation已经被重置了 之前缓存的tp信息可以被丢弃了
            if (!revokedPartitions.isEmpty()) {
                log.info("Giving away all assigned partitions as lost since generation has been reset," +
                    "indicating that consumer is no longer part of the group");
                // 用户可能会手动抛出某个exception  这里捕获并返回
                exception = invokePartitionsLost(revokedPartitions);

                // 更新此时本节点分配到的所有tp 也就是清空
                subscriptions.assignFromSubscribed(Collections.emptySet());
            }
        } else {
            // 代表此前已经有coordinator下发的group信息了
            switch (protocol) {
                // 代表单个节点来决定
                case EAGER:
                    // revoke all partitions
                    // 这里清空了之前所有的tp信息
                    revokedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    exception = invokePartitionsRevoked(revokedPartitions);

                    subscriptions.assignFromSubscribed(Collections.emptySet());

                    break;

                    // 多节点协调决定 也就是follower允许有自己的意见 或者说保留之前已有的tp
                case COOPERATIVE:
                    // only revoke those partitions that are not in the subscription any more.
                    Set<TopicPartition> ownedPartitions = new HashSet<>(subscriptions.assignedPartitions());
                    // 只需要去除不再被订阅的tp
                    revokedPartitions = ownedPartitions.stream()
                        .filter(tp -> !subscriptions.subscription().contains(tp.topic()))
                        .collect(Collectors.toSet());

                    if (!revokedPartitions.isEmpty()) {
                        exception = invokePartitionsRevoked(revokedPartitions);

                        ownedPartitions.removeAll(revokedPartitions);
                        subscriptions.assignFromSubscribed(ownedPartitions);
                    }

                    break;
            }
        }

        // 即将重新发起join的节点需要丢弃之前的角色 比如isLeader发生网络分区 现在重新加入 就要将自己作为follower
        isLeader = false;
        // 即将重新加入group 就可以将之前缓存的有关group下订阅的所有topic的信息丢弃
        subscriptions.resetGroupSubscription();

        if (exception != null) {
            throw new KafkaException("User rebalance callback throws an error", exception);
        }
    }

    /**
     * 当消费者不再需要订阅数据时 可以从与其他节点的交互中脱离
     */
    @Override
    public void onLeavePrepare() {
        // Save the current Generation and use that to get the memberId, as the hb thread can change it at any time
        final Generation currentGeneration = generation();
        final String memberId = currentGeneration.memberId;

        log.debug("Executing onLeavePrepare with generation {} and memberId {}", currentGeneration, memberId);

        // we should reset assignment and trigger the callback before leaving group
        Set<TopicPartition> droppedPartitions = new HashSet<>(subscriptions.assignedPartitions());

        if (subscriptions.hasAutoAssignedPartitions() && !droppedPartitions.isEmpty()) {
            final Exception e;
            if (generation() == Generation.NO_GENERATION || rebalanceInProgress()) {
                e = invokePartitionsLost(droppedPartitions);
            } else {
                e = invokePartitionsRevoked(droppedPartitions);
            }

            subscriptions.assignFromSubscribed(Collections.emptySet());

            if (e != null) {
                throw new KafkaException("User rebalance callback throws an error", e);
            }
        }
    }

    /**
     * 此时已经获取到了coordinator的地址 判断是否需要重新发起join请求
     * @throws KafkaException if the callback throws exception
     */
    @Override
    public boolean rejoinNeededOrPending() {
        // 非auto模式 不需要处理
        if (!subscriptions.hasAutoAssignedPartitions())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed;
        // also for those owned-but-no-longer-existed partitions we should drop them as lost
        // 检测之前缓存在本地的tp信息,也就是分配到该消费者的tp信息 与最近一次元数据相比发生了变化   group下consumer的分配就需要调整了 申请重新加入group
        if (assignmentSnapshot != null && !assignmentSnapshot.matches(metadataSnapshot)) {
            log.info("Requesting to re-join the group and trigger rebalance since the assignment metadata has changed from {} to {}",
                    assignmentSnapshot, metadataSnapshot);

            // 发出一个加入group的请求
            requestRejoin();
            return true;
        }

        // we need to join if our subscription has changed since the last join
        // 针对上一次订阅的信息 此时修改了订阅信息 也需要通知到coordinator
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) {
            log.info("Requesting to re-join the group and trigger rebalance since the subscription has changed from {} to {}",
                joinedSubscription, subscriptions.subscription());

            requestRejoin();
            return true;
        }

        // 返回此时是否需要发起join请求 或者是否正在等待结果
        return super.rejoinNeededOrPending();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    public boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        final Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(initializingPartitions, timer);
        if (offsets == null) return false;

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata != null) {
                // first update the epoch if necessary
                entry.getValue().leaderEpoch().ifPresent(epoch -> this.metadata.updateLastSeenEpochIfNewer(entry.getKey(), epoch));

                // it's possible that the partition is no longer assigned when the response is received,
                // so we need to ignore seeking if that's the case
                if (this.subscriptions.isAssigned(tp)) {
                    final ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);
                    final SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                            offsetAndMetadata.offset(), offsetAndMetadata.leaderEpoch(),
                            leaderAndEpoch);

                    this.subscriptions.seekUnvalidated(tp, position);

                    log.info("Setting offset for partition {} to the committed offset {}", tp, position);
                } else {
                    log.info("Ignoring the returned {} since its partition {} is no longer assigned",
                        offsetAndMetadata, tp);
                }
            }
        }
        return true;
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     *
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset or null if the operation timed out
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(final Set<TopicPartition> partitions,
                                                                        final Timer timer) {
        if (partitions.isEmpty()) return Collections.emptyMap();

        final Generation generationForOffsetRequest = generationIfStable();
        if (pendingCommittedOffsetRequest != null &&
            !pendingCommittedOffsetRequest.sameRequest(partitions, generationForOffsetRequest)) {
            // if we were waiting for a different request, then just clear it.
            pendingCommittedOffsetRequest = null;
        }

        do {
            if (!ensureCoordinatorReady(timer)) return null;

            // contact coordinator to fetch committed offsets
            final RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future;
            if (pendingCommittedOffsetRequest != null) {
                future = pendingCommittedOffsetRequest.response;
            } else {
                future = sendOffsetFetchRequest(partitions);
                pendingCommittedOffsetRequest = new PendingCommittedOffsetRequest(partitions, generationForOffsetRequest, future);
            }
            client.poll(future, timer);

            if (future.isDone()) {
                pendingCommittedOffsetRequest = null;

                if (future.succeeded()) {
                    return future.value();
                } else if (!future.isRetriable()) {
                    throw future.exception();
                } else {
                    timer.sleep(rebalanceConfig.retryBackoffMs);
                }
            } else {
                return null;
            }
        } while (timer.notExpired());
        return null;
    }

    /**
     * Return the consumer group metadata.
     *
     * @return the current consumer group metadata
     */
    public ConsumerGroupMetadata groupMetadata() {
        return groupMetadata;
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public void close(final Timer timer) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync(timer);
            while (pendingAsyncCommits.get() > 0 && timer.notExpired()) {
                ensureCoordinatorReady(timer);
                client.poll(timer);
                invokeCompletedOffsetCommitCallbacks();
            }
        } finally {
            super.close(timer);
        }
    }

    /**
     * 拉取所有有关提交偏移量的回调对象 并执行
     */
    void invokeCompletedOffsetCommitCallbacks() {
        // 已经禁止本实例提交偏移量了
        if (asyncCommitFenced.get()) {
            throw new FencedInstanceIdException("Get fenced exception for group.instance.id "
                + rebalanceConfig.groupInstanceId.orElse("unset_instance_id")
                + ", current member.id is " + memberId());
        }
        while (true) {
            // 从链表中不断获取回调对象 并执行
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            completion.invoke();
        }
    }

    /**
     * 这里的异步代表不会阻塞等到获取到coordinator
     * @param offsets
     * @param callback
     */
    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        // 先处理之前返回的结果
        invokeCompletedOffsetCommitCallbacks();

        if (!coordinatorUnknown()) {
            // 确保此时coordinator确实存在
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            // 此时还不清楚 coordinator的地址
            pendingAsyncCommits.incrementAndGet();
            // 设置监听coordinator获取结果的listener
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
    }

    /**
     * 异步提交偏移量
     * @param offsets
     * @param callback
     */
    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        // 发送提交偏移量的请求
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException) {
                    commitException = new RetriableCommitFailedException(e);
                }
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
                // 某个节点的提交可能会被强制拦下来
                if (commitException instanceof FencedInstanceIdException) {
                    asyncCommitFenced.set(true);
                }
            }
        });
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions. See the exception for more details
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     * @throws FencedInstanceIdException if a static member gets fenced
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     *         在指定时间内 将本消费者的偏移量上报给coordinator
     *         sync 与async的区别就是 会阻塞直到coordinator可用
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, Timer timer) {
        // 先处理所有有关偏移量的结果
        invokeCompletedOffsetCommitCallbacks();

        // 本次没有需要上报的offsets 直接成功
        if (offsets.isEmpty())
            return true;

        do {
            // 在指定时间内如果还没有获取到coordinator信息 无法继续处理
            if (coordinatorUnknown() && !ensureCoordinatorReady(timer)) {
                return false;
            }

            // 设置提交偏移量的请求到kafkaChannel上
            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, timer);

            // We may have had in-flight offset commits when the synchronous commit began. If so, ensure that
            // the corresponding callbacks are invoked prior to returning in order to preserve the order that
            // the offset commits were applied.
            // 这是处理异步提交commit返回的结果的回调对象
            invokeCompletedOffsetCommitCallbacks();

            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
            }

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            timer.sleep(rebalanceConfig.retryBackoffMs);
        } while (timer.notExpired());

        return false;
    }

    /**
     * 判断是否要自动提交偏移量
     * @param now
     */
    public void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            nextAutoCommitTimer.update(now);
            // 代表超过了一个间隔时间 需要发送自动提交请求
            if (nextAutoCommitTimer.isExpired()) {
                nextAutoCommitTimer.reset(autoCommitIntervalMs);
                doAutoCommitOffsetsAsync();
            }
        }
    }


    private void doAutoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        commitOffsetsAsync(allConsumedOffsets, (offsets, exception) -> {
            // 如果是可重试异常 更新deadline 这样会在更短的间隔内进行重试
            if (exception != null) {
                if (exception instanceof RetriableCommitFailedException) {
                    log.debug("Asynchronous auto-commit of offsets {} failed due to retriable error: {}", offsets,
                        exception);
                    nextAutoCommitTimer.updateAndReset(rebalanceConfig.retryBackoffMs);
                } else {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
                }
            } else {
                // 成功时仅打印日志
                log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
            }
        });
    }

    /**
     * 如果本消费者以自动提交偏移量的模式打开 会先提交偏移量
     * @param timer 超时时间
     */
    private void maybeAutoCommitOffsetsSync(Timer timer) {
        if (autoCommitEnabled) {
            // 获取需要提交偏移量的信息
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
                if (!commitOffsetsSync(allConsumedOffsets, timer))
                    log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
            }
        }
    }

    /**
     * 仅在失败时打印日志
     */
    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * NOTE: This is visible only for testing
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     * 发送提交偏移量的请求
     */
    RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 如果偏移量信息本身为空 直接返回一个成功的结果
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        // 获取coordinator的信息
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        // 以topic为单位  value存储的是每个topic下分配到本节点的所有分区的偏移量
        Map<String, OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopicDataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }

            OffsetCommitRequestData.OffsetCommitRequestTopic topic = requestTopicDataMap
                    .getOrDefault(topicPartition.topic(),
                            new OffsetCommitRequestData.OffsetCommitRequestTopic()
                                    .setName(topicPartition.topic())
                    );

            topic.partitions().add(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(topicPartition.partition())
                    .setCommittedOffset(offsetAndMetadata.offset())
                    .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setCommittedMetadata(offsetAndMetadata.metadata())
            );
            requestTopicDataMap.put(topicPartition.topic(), topic);
        }

        final Generation generation;
        if (subscriptions.hasAutoAssignedPartitions()) {
            generation = generationIfStable();
            // if the generation is null, we are not part of an active group (and we expect to be).
            // the only thing we can do is fail the commit and let the user rejoin the group in poll().
            // 代表此时group的rebalance还处于不稳定状态
            if (generation == null) {
                log.info("Failing OffsetCommit request since the consumer is not part of an active group");

                // 此时处于重分配阶段
                if (rebalanceInProgress()) {
                    // if the client knows it is already rebalancing, we can use RebalanceInProgressException instead of
                    // CommitFailedException to indicate this is not a fatal error
                    return RequestFuture.failure(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                        "consumer is undergoing a rebalance for auto partition assignment. You can try completing the rebalance " +
                        "by calling poll() and then retry the operation."));
                } else {
                    return RequestFuture.failure(new CommitFailedException("Offset commit cannot be completed since the " +
                        "consumer is not part of an active group for auto partition assignment; it is likely that the consumer " +
                        "was kicked out of the group."));
                }
            }
        } else {
            generation = Generation.NO_GENERATION;
        }

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
                new OffsetCommitRequestData()
                        .setGroupId(this.rebalanceConfig.groupId)
                        .setGenerationId(generation.generationId)
                        .setMemberId(generation.memberId)
                        .setGroupInstanceId(rebalanceConfig.groupInstanceId.orElse(null))
                        .setTopics(new ArrayList<>(requestTopicDataMap.values()))
        );

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return client.send(coordinator, builder)
                // 使用OffsetCommitResponseHandler来处理发送偏移量的结果
                .compose(new OffsetCommitResponseHandler(offsets, generation));
    }

    /**
     * 处理提交偏移量的结果
     */
    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets, Generation generation) {
            super(generation);
            this.offsets = offsets;
        }

        /**
         * 处理收到的结果
         * @param commitResponse
         * @param future
         */
        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitSensor.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            // 结果也是以topic为单位
            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : commitResponse.data().topics()) {
                // 获取有关每个分区的提交结果
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                    OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);

                    long offset = offsetAndMetadata.offset();

                    Errors error = Errors.forCode(partition.errorCode());
                    // 本次提交成功
                    if (error == Errors.NONE) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                    } else {
                        // 本次返回的是可重试异常
                        if (error.exception() instanceof RetriableException) {
                            log.warn("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        } else {
                            log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());
                        }

                        if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                            future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                            return;
                        } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                            unauthorizedTopics.add(tp.topic());
                        } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                                || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                            // raise the error to the user
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS
                                || error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                            // just retry
                            future.raise(error);
                            return;
                        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                                || error == Errors.NOT_COORDINATOR
                                || error == Errors.REQUEST_TIMED_OUT) {
                            markCoordinatorUnknown(error);
                            future.raise(error);
                            return;
                        } else if (error == Errors.FENCED_INSTANCE_ID) {
                            log.info("OffsetCommit failed with {} due to group instance id {} fenced", sentGeneration, rebalanceConfig.groupInstanceId);

                            // if the generation has changed or we are not in rebalancing, do not raise the fatal error but rebalance-in-progress
                            if (generationUnchanged()) {
                                future.raise(error);
                            } else {
                                KafkaException exception;
                                synchronized (ConsumerCoordinator.this) {
                                    if (ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                        exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                            "consumer member's old generation is fenced by its group instance id, it is possible that " +
                                            "this consumer has already participated another rebalance and got a new generation");
                                    } else {
                                        exception = new CommitFailedException();
                                    }
                                }
                                future.raise(exception);
                            }
                            return;
                        } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                            /* Consumer should not try to commit offset in between join-group and sync-group,
                             * and hence on broker-side it is not expected to see a commit offset request
                             * during CompletingRebalance phase; if it ever happens then broker would return
                             * this error to indicate that we are still in the middle of a rebalance.
                             * In this case we would throw a RebalanceInProgressException,
                             * request re-join but do not reset generations. If the callers decide to retry they
                             * can go ahead and call poll to finish up the rebalance first, and then try commit again.
                             */
                            requestRejoin();
                            future.raise(new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                "consumer group is executing a rebalance at the moment. You can try completing the rebalance " +
                                "by calling poll() and then retry commit again"));
                            return;
                        } else if (error == Errors.UNKNOWN_MEMBER_ID
                                || error == Errors.ILLEGAL_GENERATION) {
                            log.info("OffsetCommit failed with {}: {}", sentGeneration, error.message());

                            // only need to reset generation and re-join group if generation has not changed or we are not in rebalancing;
                            // otherwise only raise rebalance-in-progress error
                            KafkaException exception;
                            synchronized (ConsumerCoordinator.this) {
                                if (!generationUnchanged() && ConsumerCoordinator.this.state == MemberState.PREPARING_REBALANCE) {
                                    exception = new RebalanceInProgressException("Offset commit cannot be completed since the " +
                                        "consumer member's generation is already stale, meaning it has already participated another rebalance and " +
                                        "got a new generation. You can try completing the rebalance by calling poll() and then retry commit again");
                                } else {
                                    resetGenerationOnResponseError(ApiKeys.OFFSET_COMMIT, error);
                                    exception = new CommitFailedException();
                                }
                            }
                            future.raise(exception);
                            return;
                        } else {
                            future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                            return;
                        }
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder =
            new OffsetFetchRequest.Builder(this.rebalanceConfig.groupId, true, new ArrayList<>(partitions), throwOnFetchStableOffsetsUnsupported);

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        private OffsetFetchResponseHandler() {
            super(Generation.NO_GENERATION);
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch failed: {}", error.message());

                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    markCoordinatorUnknown(error);
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                }
                return;
            }

            Set<String> unauthorizedTopics = null;
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            Set<TopicPartition> unstableTxnOffsetTopicPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData partitionData = entry.getValue();
                if (partitionData.hasError()) {
                    Errors error = partitionData.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        if (unauthorizedTopics == null) {
                            unauthorizedTopics = new HashSet<>();
                        }
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.UNSTABLE_OFFSET_COMMIT) {
                        unstableTxnOffsetTopicPartitions.add(tp);
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response for partition " +
                            tp + ": " + error.message()));
                        return;
                    }
                } else if (partitionData.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch);
                    // if there's no committed offset, record as null
                    offsets.put(tp, new OffsetAndMetadata(partitionData.offset, partitionData.leaderEpoch, partitionData.metadata));
                } else {
                    log.info("Found no committed offset for partition {}", tp);
                    offsets.put(tp, null);
                }
            }

            if (unauthorizedTopics != null) {
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else if (!unstableTxnOffsetTopicPartitions.isEmpty()) {
                // just retry
                log.info("The following partitions still have unstable offsets " +
                             "which are not cleared on the broker side: {}" +
                             ", this could be either " +
                             "transactional offsets waiting for completion, or " +
                             "normal offsets waiting for replication after appending to local log", unstableTxnOffsetTopicPartitions);
                future.raise(new UnstableOffsetCommitException("There are unstable offsets for the requested topic partitions"));
            } else {
                future.complete(offsets);
            }
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitSensor;
        private final Sensor revokeCallbackSensor;
        private final Sensor assignCallbackSensor;
        private final Sensor loseCallbackSensor;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitSensor = metrics.sensor("commit-latency");
            this.commitSensor.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitSensor.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitSensor.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            this.revokeCallbackSensor = metrics.sensor("partition-revoked-latency");
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-revoked rebalance listener callback"), new Avg());
            this.revokeCallbackSensor.add(metrics.metricName("partition-revoked-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-revoked rebalance listener callback"), new Max());

            this.assignCallbackSensor = metrics.sensor("partition-assigned-latency");
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-assigned rebalance listener callback"), new Avg());
            this.assignCallbackSensor.add(metrics.metricName("partition-assigned-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-assigned rebalance listener callback"), new Max());

            this.loseCallbackSensor = metrics.sensor("partition-lost-latency");
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-avg",
                this.metricGrpName,
                "The average time taken for a partition-lost rebalance listener callback"), new Avg());
            this.loseCallbackSensor.add(metrics.metricName("partition-lost-latency-max",
                this.metricGrpName,
                "The max time taken for a partition-lost rebalance listener callback"), new Max());

            Measurable numParts = (config, now) -> subscriptions.numAssignedPartitions();
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    /**
     * 元数据快照对象
     */
    private static class MetadataSnapshot {
        private final int version;
        private final Map<String, Integer> partitionsPerTopic;

        /**
         * 基于此时的订阅状态对象生成快照数据
         * @param subscription
         * @param cluster
         * @param version
         */
        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster, int version) {
            // 此时订阅的每个topic下的partition数量
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.metadataTopics()) {
                Integer numPartitions = cluster.partitionCountForTopic(topic);
                if (numPartitions != null)
                    partitionsPerTopic.put(topic, numPartitions);
            }
            this.partitionsPerTopic = partitionsPerTopic;
            this.version = version;
        }

        boolean matches(MetadataSnapshot other) {
            return version == other.version || partitionsPerTopic.equals(other.partitionsPerTopic);
        }

        @Override
        public String toString() {
            return "(version" + version + ": " + partitionsPerTopic + ")";
        }
    }

    /**
     * 有关提交偏移量后的回调对象 会在某个实际加入到链表中
     * 可以看到偏移量也是批量提交的
     */
    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

    /* test-only classes below */
    RebalanceProtocol getProtocol() {
        return protocol;
    }

    boolean poll(Timer timer) {
        return poll(timer, true);
    }
}
