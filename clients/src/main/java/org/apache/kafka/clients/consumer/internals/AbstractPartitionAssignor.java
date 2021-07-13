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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 * 作为分配器的骨架类
 */
public abstract class AbstractPartitionAssignor implements ConsumerPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    /**
     * Perform the group assignment given the partition counts and member subscriptions
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     *                           from this map.
     * @param subscriptions Map from the member id to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     */
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, Subscription> subscriptions);

    /**
     * 这里会产生分配的结果
     * @param metadata Current topic/broker metadata known by consumer   维护了此时所有topic下的分区信息
     * @param groupSubscription Subscriptions from all members including metadata provided through {@link #subscriptionUserData(Set)}
     *                          描述了当前消费者组订阅的所有topic 以及每个member可能想要保留的tp
     * @return
     */
    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();

        // 抽取出group下订阅的所有topic
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());

        // 获取此时被订阅的topic下的所有分区
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                // 没有分区信息的topic会被忽略
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        // 由子类来实现分配
        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);

        // this class maintains no user data, so just wrap the results
        // 将结果包装成 GroupAssignment
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return new GroupAssignment(assignments);
    }

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
    }

    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
    }

    public static class MemberInfo implements Comparable<MemberInfo> {
        public final String memberId;
        public final Optional<String> groupInstanceId;

        public MemberInfo(String memberId, Optional<String> groupInstanceId) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
        }

        @Override
        public int compareTo(MemberInfo otherMemberInfo) {
            if (this.groupInstanceId.isPresent() &&
                    otherMemberInfo.groupInstanceId.isPresent()) {
                return this.groupInstanceId.get()
                        .compareTo(otherMemberInfo.groupInstanceId.get());
            } else if (this.groupInstanceId.isPresent()) {
                return -1;
            } else if (otherMemberInfo.groupInstanceId.isPresent()) {
                return 1;
            } else {
                return this.memberId.compareTo(otherMemberInfo.memberId);
            }
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MemberInfo && this.memberId.equals(((MemberInfo) o).memberId);
        }

        /**
         * We could just use member.id to be the hashcode, since it's unique
         * across the group.
         */
        @Override
        public int hashCode() {
            return memberId.hashCode();
        }

        @Override
        public String toString() {
            return "MemberInfo [member.id: " + memberId
                    + ", group.instance.id: " + groupInstanceId.orElse("{}")
                    + "]";
        }
    }
}
