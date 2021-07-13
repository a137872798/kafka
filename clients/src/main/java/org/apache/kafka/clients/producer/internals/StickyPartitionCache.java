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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Utils;

/**
 * An internal class that implements a cache used for sticky partitioning behavior. The cache tracks the current sticky
 * partition for any given topic. This class should not be used externally.
 * 粘性分区对象
 */
public class StickyPartitionCache {

    /**
     * 基于某个分区键 总是返回同一个分区值
     */
    private final ConcurrentMap<String, Integer> indexCache;

    public StickyPartitionCache() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    /**
     * 基于topic信息和集群信息 返回分区
     * @param topic
     * @param cluster
     * @return
     */
    public int partition(String topic, Cluster cluster) {
        // 如果之前已经计算出分区了 即使集群发生变化 还是返回同一个分区
        Integer part = indexCache.get(topic);
        if (part == null) {
            // 代表还未产生分区 这里开始计算
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    /**
     * 如果强制调用了该方法 就代表希望基于最新的集群信息计算分区
     * @param topic
     * @param cluster 当前集群信息  集群中记录了某个topic下总计有多少分区
     * @param prevPartition 可以手动传入上次使用的分区
     * @return
     */
    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        // 从集群中获取该topic下的所有分区
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        // 从缓存中获取之前的分区
        Integer oldPart = indexCache.get(topic);
        Integer newPart = oldPart;
        // Check that the current sticky partition for the topic is either not set or that the partition that 
        // triggered the new batch matches the sticky partition that needs to be changed.
        // 如果本次传入的就是之前返回的分区,就代表使用者的意图是期望重新计算分区(使用者肯定是传入了之前获取到的分区)  如果之前分区为空 本次自然就要计算分区
        if (oldPart == null || oldPart == prevPartition) {
            // 先找到所有可用的分区
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            // 此时没有可用的分区 会随机返回一个新的分区
            if (availablePartitions.size() < 1) {
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();
                // 只有一个可用分区 直接使用
            } else if (availablePartitions.size() == 1) {
                newPart = availablePartitions.get(0).partition();
            } else {
                // 此时可能有一组可用的分区 这里会随机选择一个 这里有一个问题 多个生产者可能会将消息发往某个topic下的同一个分区么
                while (newPart == null || newPart.equals(oldPart)) {
                    int random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }
            // Only change the sticky partition if it is null or prevPartition matches the current sticky partition.
            if (oldPart == null) {
                indexCache.putIfAbsent(topic, newPart);
            } else {
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }
        // 其他意图直接返回缓存的分区
        return indexCache.get(topic);
    }

}