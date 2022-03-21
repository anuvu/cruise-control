/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 * adding due to checkstyle 
 */
package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.closeAdminClientWithTimeout;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.createAdminClient;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.parseAdminClientConfigs;

/***
 * Sometimes, replicas are unable to be updated to in-sync state to zookeeper
 * This happens during kafka-zk and inter-zk network outages. This leads to a state 
 * where the paritions are forever stuck in an under-replicated state.
 * See KAFKA-1407, KAFKA-3042 for more information. This goal aims to fix this 
 * by detecting replicas in this state and reassigning replicas to the same brokers. 
 */
public class LaggingReplicaReassignmentGoal extends AbstractGoal {

    private static final Logger LOG = LoggerFactory.getLogger(LaggingReplicaReassignmentGoal.class);

    protected static final ConcurrentHashMap<PartitionInfoWrapper, Long> LAGGING_PARTITIONS_MAP = new ConcurrentHashMap<PartitionInfoWrapper, Long>();
    private static final Object UPDATE_LAGGING_PARTITIONS_MAP_LOCK = new Object();

    private static volatile boolean laggingRecoveryNeeded = false;

    private static List<PartitionInfo> laggingPartitionsList = Collections.synchronizedList(new ArrayList<PartitionInfo>());

    private KafkaCruiseControlConfig _parsedConfig;

    private long _maxReplicaLagMs;

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("Configuring LaggingReplicaReassignmentGoal");
        _parsedConfig = new KafkaCruiseControlConfig(configs, false);
        _balancingConstraint = new BalancingConstraint(_parsedConfig);
        _numWindows = _parsedConfig.getInt(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
        _minMonitoredPartitionPercentage = _parsedConfig.getDouble(MonitorConfig.MIN_VALID_PARTITION_RATIO_CONFIG);
        _maxReplicaLagMs = (long) configs.get(AnalyzerConfig.MAX_LAGGING_REPLICA_REASSIGN_MS);
    }

    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
        switch (action.balancingAction()) {
            case INTER_BROKER_REPLICA_MOVEMENT:
                return ActionAcceptance.ACCEPT;
            default:
              return ActionAcceptance.ACCEPT;
        }
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new ClusterModelStatsComparator() {

            private StringBuffer _reason = new StringBuffer();

            @Override
            public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
                int s1 = stats1.numPartitionsWithLaggingReplicas();
                int s2 = stats2.numPartitionsWithLaggingReplicas();
                _reason.setLength(0);
                if (s1 > s2) {
                    _reason.append("Number of partitions with lagging replicas increased from " + s2 + " to " + s1);
                    return -1;
                } else {
                    _reason.append("Number of partitions with lagging replicas decreased from " + s2 + " to " + s1);
                    return 0;
                }
            }

            @Override
            public String explainLastComparison() {
                return _reason.toString();
            }

        };
    }

    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
        return new ModelCompletenessRequirements(GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
    }

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    public String name() {
        return LaggingReplicaReassignmentGoal.class.getSimpleName();
    }

    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
        return false;
    }

    @Override
    protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        LOG.info("initGoalState");
        checkIfReplicasLagging(clusterModel);
    }

    @Override
    protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        
        LOG.info("updateGoalState");
        checkIfReplicasLagging(clusterModel);
        if (!laggingRecoveryNeeded) {
            finish();
        }

    }

    void checkIfReplicasLagging(ClusterModel clusterModel) throws OptimizationFailureException {
        long currentTimeMillis = System.currentTimeMillis();
        ConcurrentHashMap<PartitionInfoWrapper, Long> newLaggingPartitionsMap = new ConcurrentHashMap<PartitionInfoWrapper, Long>();
        LOG.info("Checking for lagging replicas");
        //List<PartitionInfo> laggingPartitionInfos = clusterModel.getPartitionsWithLaggingReplicas();
        //LAGGING_PARTITIONS_MAP.entrySet().removeIf(e -> !laggingPartitionInfos.contains(e.getKey()._pi));
        for (PartitionInfo partition: clusterModel.getPartitionsWithLaggingReplicas()) {
            LOG.info(partition.toString());
            PartitionInfoWrapper piw = new PartitionInfoWrapper(partition);
            long lastSeenTime = LAGGING_PARTITIONS_MAP.getOrDefault(piw, currentTimeMillis);
            if (currentTimeMillis - lastSeenTime >= _maxReplicaLagMs) {
                LOG.info("Partition {} has been lagging for past {} minutes", partition.toString(), 
                            (currentTimeMillis - lastSeenTime) / (60 * 1000));
                laggingRecoveryNeeded = true;
                laggingPartitionsList.add(partition);
            }
            newLaggingPartitionsMap.put(piw, lastSeenTime);
        }
        synchronized (UPDATE_LAGGING_PARTITIONS_MAP_LOCK) {
            LAGGING_PARTITIONS_MAP.clear();
            LAGGING_PARTITIONS_MAP.putAll(newLaggingPartitionsMap);
        }
        LOG.info("Lagging partitions map: {}  on thread after {}", LAGGING_PARTITIONS_MAP.toString(), Thread.currentThread().getName());
    }

    List<PartitionInfo> getLaggingPartitions() {
        return laggingPartitionsList;
    }

    @Override
    protected void rebalanceForBroker(Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions) throws OptimizationFailureException {
        
        LOG.info("Current lagging partitions: {} ", laggingPartitionsList.toString());
        if (laggingRecoveryNeeded) {
            synchronized (UPDATE_LAGGING_PARTITIONS_MAP_LOCK) {
                Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>(); 
                // gather all the lagging partitions into a reassignments map
                for (PartitionInfo laggingPartition: laggingPartitionsList) {
                    List<Node> laggingReplicas = new LinkedList<Node>(Arrays.asList(laggingPartition.replicas()));
                    reassignments.put(new TopicPartition(laggingPartition.topic(), laggingPartition.partition()), 
                    Optional.of(new NewPartitionReassignment(laggingReplicas.stream().map(node -> node.id()).collect(Collectors.toList()))));
                    //LAGGING_PARTITIONS_MAP.remove(new PartitionInfoWrapper(laggingPartition));
                }
                // use admin client to move them to same brokers
                LOG.info("Creating adminClient for partition reassignment");
                AdminClient adminClient = createAdminClient(parseAdminClientConfigs(_parsedConfig));
                try {
                    adminClient.alterPartitionReassignments(reassignments).all().get();
                    reassignments.entrySet().stream().
                                            forEach(e -> LOG.info("Moved partition {}: {}", e.getKey(), e.getValue().get().targetReplicas()));
                    laggingPartitionsList.clear();
                    laggingRecoveryNeeded = false;
                    LAGGING_PARTITIONS_MAP.clear();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("Unable to move replicas onto same brokers");
                }
                LOG.info("Closing adminClient");
                closeAdminClientWithTimeout(adminClient);
            }
        }
        
    }
    protected static class PartitionInfoWrapper {

        private PartitionInfo _pi;

        public PartitionInfoWrapper(PartitionInfo pi) {
            this._pi = pi;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof PartitionInfoWrapper)) {
                return false;
            }
            PartitionInfoWrapper partitionInfoWrapperObj = (PartitionInfoWrapper) o;
            PartitionInfo p2 = partitionInfoWrapperObj._pi;
            if (_pi.topic().equals(p2.topic()) && _pi.partition() == p2.partition() 
                && (
                    (_pi.leader() == null && p2.leader() == null) 
                    || (_pi.leader() != null && p2.leader() != null && _pi.leader().id() == p2.leader().id())
                    )
                && _pi.inSyncReplicas().length == p2.inSyncReplicas().length) {
                Set<Integer> p2ISRSet = Arrays.stream(p2.inSyncReplicas()).map(isr -> isr.id()).collect(Collectors.toSet());
                if (Arrays.stream(_pi.inSyncReplicas()).allMatch(isr -> p2ISRSet.contains(isr.id()))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((_pi.topic() == null) ? 0 : _pi.topic().hashCode());
            result = prime * result + _pi.partition();
            result = prime * result + ((_pi.leader() == null) ? 0 : _pi.leader().id());
            for (Node n: _pi.inSyncReplicas()) {
                result = prime * result + n.id();
            }
            return result;
        }

        @Override
        public String toString() {
            return _pi.toString();
        }

    }
    
}
