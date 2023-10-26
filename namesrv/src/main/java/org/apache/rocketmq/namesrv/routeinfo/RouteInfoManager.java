/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * 路由管理器
 */
public class RouteInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    // 针对 Broker、Topic 增删改查的读写锁
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // 保存Topic和队列信息，也叫路由信息
    private final HashMap<String/* topic */, Map<String /* brokerName */ , QueueData>> topicQueueTable;
    // 存储brokerName和队列信息
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;
    // 集群和broker关系
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    // 在线的broker和Broker信息对应关系
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
    // 过滤服务器信息
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<>(1024);
        this.brokerAddrTable = new HashMap<>(128);
        this.clusterAddrTable = new HashMap<>(32);
        this.brokerLiveTable = new HashMap<>(256);
        this.filterServerTable = new HashMap<>(256);
    }

    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper;
    }

    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    public void deleteTopic(final String topic, final String clusterName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (brokerNames != null
                    && !brokerNames.isEmpty()) {
                    Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
                    if (queueDataMap != null) {
                        for (String brokerName : brokerNames) {
                            final QueueData removedQD = queueDataMap.remove(brokerName);
                            if (removedQD != null) {
                                log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, topic,
                                    removedQD);
                            }
                        }
                        if (queueDataMap.isEmpty()) {
                            log.info("deleteTopic, remove the topic all queue {} {}", clusterName, topic);
                            this.topicQueueTable.remove(topic);
                        }
                    }

                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    /**
     * Broker 注册的核心逻辑如下：
     *
     * 向集群关系表 clusterAddrTable 添加 Broker 组名；
     * 从Broker表 brokerAddrTable 获取或创建Broker组 BrokerData；
     * 遍历Broker组里的Broker表，如果Broker地址一样，但ID不一样，可能是由于从主切换重新注册，因此需要先移除旧的Broker；
     * 把Broker添加到Broker组里；
     * 如果当前是注册的 Master Broker(brokerId=0)，且是第一次注册或版本发生变更，就创建或更新当前Broker组的消息队列配置。
     * 接着创建了NameServer与Broker间的连接保活 BrokerLiveInfo 信息；
     * 接着添加或更新 FilterServer 列表；
     * 最后，如果是 Slave Broker，返回 Master Broker 的地址和HA地址；
     * @param clusterName broker 集群名称
     * @param brokerAddr broker 机器地址
     * @param brokerName broker 组名称
     * @param brokerId 当前 broker 唯一ID
     * @param haServerAddr HA 地址
     * @param topicConfigWrapper topic 配置
     * @param filterServerList FilterServer
     * @param channel 网络长连接通道
     * @return
     */
    public RegisterBrokerResult registerBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final TopicConfigSerializeWrapper topicConfigWrapper,
            final List<String> filterServerList,
            final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                this.lock.writeLock().lockInterruptibly();

                // 1.初始化或者更新集群中brokerNames信息
                Set<String> brokerNames = this.clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>());
                brokerNames.add(brokerName);

                boolean registerFirst = false;

                // 如果brokerData不存在则说明是第一次创建
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    registerFirst = true;
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                    this.brokerAddrTable.put(brokerName, brokerData);
                }

                Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
                // 如果当前brokerId小于最小，说明主从切换了
                Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> item = it.next();
                    // 如果主从切换了,要先删除brokerAddrsMap中旧的broker信息
                    if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                        log.debug("remove entry {} from brokerData", item);
                        it.remove();
                    }
                }
                // 将当前broker信息更新到brokerAddrsMap中
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                if (MixAll.MASTER_ID == brokerId) {
                    log.info("cluster [{}] brokerName [{}] master address change from {} to {}", brokerData.getCluster(), brokerData.getBrokerName(), oldAddr, brokerAddr);
                }

                registerFirst = registerFirst || (null == oldAddr);

                // 3.更新路由元数据
                if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {
                    // 如果Topic配置信息发生变更或者该broker为第一次注册
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                        ConcurrentMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                // 根据brokername及topicconfig（read、write queue数量等）新增或者更新到topicQueueTable中
                                this.createAndUpdateQueueData(brokerName, entry.getValue());
                            }
                        }
                    }
                }

                //更新brokerLiveTable中当前broker的存活信息
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr, new BrokerLiveInfo( System.currentTimeMillis(), topicConfigWrapper.getDataVersion(), channel, haServerAddr));
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                //更新过滤server信息
                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                if (MixAll.MASTER_ID != brokerId) {
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }

    public boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    public void updateBrokerInfoUpdateTimestamp(final String brokerAddr, long timeStamp) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            prev.setLastUpdateTimestamp(timeStamp);
        }
    }

    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(queueData.getBrokerName(), queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            QueueData old = queueDataMap.put(queueData.getBrokerName(), queueData);
            if (old != null && !old.equals(queueData)) {
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), old,
                        queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        return operateWritePermOfBrokerByLock(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
    }

    public int addWritePermOfBrokerByLock(final String brokerName) {
        return operateWritePermOfBrokerByLock(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
    }

    private int operateWritePermOfBrokerByLock(final String brokerName, final int requestCode) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, requestCode);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("operateWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }


    private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
        int topicCnt = 0;

        for (Map.Entry<String, Map<String, QueueData>> entry : topicQueueTable.entrySet()) {
            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();

            if (queueDataMap != null) {
                QueueData qd = queueDataMap.get(brokerName);
                if (qd != null) {
                    int perm = qd.getPerm();
                    switch (requestCode) {
                        case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                            perm &= ~PermName.PERM_WRITE;
                            break;
                        case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                            perm = PermName.PERM_READ | PermName.PERM_WRITE;
                            break;
                    }
                    qd.setPerm(perm);

                    topicCnt++;
                }
            }
        }

        return topicCnt;
    }

    /**
     * Broker 下线的逻辑
     *
     * 从 brokerLiveTable 移除连接保活信息；
     * 从 filterServerTable 移除 FilterServer 列表；
     * 从 brokerAddrTable 下的 BrokerData 移除 Broker；
     * 如果 BrokerData 没有 Broker 了，从 brokerAddrTable 移除 Broker 组；
     * 如果 Broker 组移除了，从 clusterAddrTable 中移除 Broker 组名；
     * 如果整个集群下的没有 Broker 组了，从 clusterAddrTable 中移除集群，最后移除 Broker 消息队列。
     *
     * @param clusterName 集群名称
     * @param brokerAddr Broker地址
     * @param brokerName Broker 组名
     * @param brokerId Broker ID
     */
    public void unregisterBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                // 1.存活的brokerLiveTable要删除brokerAddrInfo
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                        brokerLiveInfo != null ? "OK" : "Failed",
                        brokerAddr
                );

                // 2.过滤service删除BrokerAddrInfo
                this.filterServerTable.remove(brokerAddr);

                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    //  删除brokerData中要下线的broker地址
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                            addr != null ? "OK" : "Failed",
                            brokerAddr
                    );

                    // 如果broker地址为空，说明当前broker所有节点都宕机了
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                                brokerName
                        );

                        removeBrokerName = true;
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                                removed ? "OK" : "Failed",
                                brokerName);

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                    clusterName
                            );
                        }
                    }
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }

    private void removeTopicByBrokerName(final String brokerName) {
        Set<String> noBrokerRegisterTopic = new HashSet<>();

        // 相同brokerName都宕机，则会清除queueData
        this.topicQueueTable.forEach((topic, queueDataMap) -> {
            QueueData old = queueDataMap.remove(brokerName);
            if (old != null) {
                log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, old);
            }

            if (queueDataMap.size() == 0) {
                // 如果是主节点，则会把brokerAddrTable置为不可写状态
                noBrokerRegisterTopic.add(topic);
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
            }
        });

        noBrokerRegisterTopic.forEach(topicQueueTable::remove);
    }

    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<>();
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
                if (queueDataMap != null) {
                    topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                    foundQueueData = true;

                    brokerNameSet.addAll(queueDataMap.keySet());

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData
                                    .getBrokerAddrs().clone());
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;

                            // skip if filter server table is empty
                            if (!filterServerTable.isEmpty()) {
                                for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                    List<String> filterServerList = this.filterServerTable.get(brokerAddr);

                                    // only add filter server list when not null
                                    if (filterServerList != null) {
                                        filterServerMap.put(brokerAddr, filterServerList);
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    /**
     * 遍历brokerLiveTable，如果当前时间大于broker最后更新时间+超时时间(默认是120s)，
     * 也就是说namesrv距离最后一次收到broker的心跳已经超过了120s。
     * namesrv会主动关闭与broker的channel，并且发送broker注销请求。
     * @return
     */
    public int scanNotActiveBroker() {
        int removeCount = 0;
        // 遍历存活brokerTable
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            // 默认超过2min未更新就判断失效，关闭 Channel、移除 Broker
            if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                // 关闭channel
                RemotingUtil.closeChannel(next.getValue().getChannel());
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                // 发送broker注销请求
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());

                removeCount++;
            }
        }

        return removeCount;
    }

    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;
        //获取BrokerAddr
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable = this.brokerLiveTable.entrySet().iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {
            try {
                try {
                    this.lock.writeLock().lockInterruptibly();
                    // 1.从brokerLiveTable移除宕机的broker信息
                    this.brokerLiveTable.remove(brokerAddrFound);
                    // 2.filterServerTable移除宕机的broker信息
                    this.filterServerTable.remove(brokerAddrFound);
                    String brokerNameFound = null;
                    boolean removeBrokerName = false;
                    // 3.从brokerAddrTable移除宕机的broker信息
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();
                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed", brokerId, brokerAddr);
                                break;
                            }
                        }

                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed", brokerData.getBrokerName());
                        }
                    }

                    // 4.clusterAddrTable判断是否有空集群需要移除
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed", brokerNameFound, clusterName);
                                if (brokerNames.isEmpty()) {
                                    log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster", clusterName);
                                    it.remove();
                                }
                                break;
                            }
                        }
                    }

                    // 5.从brokerAddrTable移除宕机的broker信息
                    if (removeBrokerName) {
                        String finalBrokerNameFound = brokerNameFound;
                        Set<String> needRemoveTopic = new HashSet<>();
                        topicQueueTable.forEach((topic, queueDataMap) -> {
                            QueueData old = queueDataMap.remove(finalBrokerNameFound);
                            log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed", topic, old);
                            if (queueDataMap.size() == 0) {
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed", topic);
                                needRemoveTopic.add(topic);
                            }
                        });
                        needRemoveTopic.forEach(topicQueueTable::remove);
                    }
                } finally {
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, Map<String, QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Map<String, QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public TopicList getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                    topicList.getTopicList().add(entry.getKey());
                    topicList.getTopicList().addAll(entry.getValue());
                }

                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    public TopicList getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    this.topicQueueTable.forEach((topic, queueDataMap) -> {
                        if (queueDataMap.containsKey(brokerName)) {
                            topicList.getTopicList().add(topic);
                        }
                    });
                }

            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }

    public TopicList getUnitTopics() {
        return topicQueueTableIter(qd -> TopicSysFlag.hasUnitFlag(qd.getTopicSysFlag()));
    }

    public TopicList getHasUnitSubTopicList() {
        return topicQueueTableIter(qd -> TopicSysFlag.hasUnitSubFlag(qd.getTopicSysFlag()));
    }

    public TopicList getHasUnitSubUnUnitTopicList() {
        return topicQueueTableIter(qd -> !TopicSysFlag.hasUnitFlag(qd.getTopicSysFlag())
                && TopicSysFlag.hasUnitSubFlag(qd.getTopicSysFlag()));
    }

    private TopicList topicQueueTableIter(Predicate<QueueData> pickCondition) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                topicQueueTable.forEach((topic, queueDataMap) -> {
                    for (QueueData qd : queueDataMap.values()) {
                        if (pickCondition.test(qd)) {
                            topicList.getTopicList().add(topic);
                        }

                        // we need only one queue data here
                        break;
                    }
                });

            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList;
    }
}

class BrokerLiveInfo {
    // Broker 最近一次的心跳时间
    private long lastUpdateTimestamp;
    // Broker 数据版本号
    private DataVersion dataVersion;
    // 与 Broker 间的网络长连接
    private Channel channel;
    // HA高可用节点地址
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel,
                          String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
                + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
