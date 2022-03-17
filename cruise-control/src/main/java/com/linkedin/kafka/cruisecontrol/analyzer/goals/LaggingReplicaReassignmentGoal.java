/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 * adding due to checkstyle 
 */
package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import java.util.Arrays;
import java.util.ArrayList;
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
import com.linkedin.kafka.cruisecontrol.model.PartitionInfoWrapper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private boolean _laggingRecoveryNeeded;

    private List<PartitionInfo> _laggingPartitions;

    protected HashMap<PartitionInfoWrapper, Long> _laggingPartitionsMap;

    private HashMap<PartitionInfoWrapper, Long> _newLaggingPartitionsMap;

    private long _maxReplicaLagMs;

    private AdminClient _adminClient;

    private KafkaCruiseControlConfig _parsedConfig;

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("Configuring LaggingReplicaReassignmentGoal");
        _parsedConfig = new KafkaCruiseControlConfig(configs, false);
        _adminClient = createAdminClient(parseAdminClientConfigs(_parsedConfig));
        _balancingConstraint = new BalancingConstraint(_parsedConfig);
        _numWindows = _parsedConfig.getInt(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
        _minMonitoredPartitionPercentage = _parsedConfig.getDouble(MonitorConfig.MIN_VALID_PARTITION_RATIO_CONFIG);
        // _laggingPartitionsMap = new HashMap<PartitionInfoWrapper, Long>();
        _maxReplicaLagMs = (long) configs.get(AnalyzerConfig.MAX_LAGGING_REPLICA_REASSIGN_MS);
        _laggingPartitions = new ArrayList<PartitionInfo>();
        _laggingRecoveryNeeded = false;
        LOG.info("Configured LaggingReplicaReassignmentGoal");
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
        if (!_laggingRecoveryNeeded) {
            finish();
        }

    }

    void checkIfReplicasLagging(ClusterModel clusterModel) throws OptimizationFailureException {
        long currentTimeMillis = System.currentTimeMillis();
        _newLaggingPartitionsMap = new HashMap<PartitionInfoWrapper, Long>();
        LOG.info("Checking for lagging replicas");
        LOG.info("Admin client details {}, {}", _adminClient.toString(), _adminClient.hashCode());
        _laggingPartitionsMap = clusterModel.getLaggingPartitionsMap();
        for (PartitionInfo partition: clusterModel.getPartitionsWithLaggingReplicas()) {
            LOG.info(partition.toString());
            PartitionInfoWrapper piw = new PartitionInfoWrapper(partition);
            long lastSeenTime = _laggingPartitionsMap.getOrDefault(piw, currentTimeMillis);
            if (currentTimeMillis - lastSeenTime >= _maxReplicaLagMs) {
                LOG.info("Partition {} has been lagging for past {} minutes", partition.toString(), (currentTimeMillis - lastSeenTime) / (60 * 1000));
                _laggingRecoveryNeeded = true;
                _laggingPartitions.add(partition);
            }
            _newLaggingPartitionsMap.put(piw, lastSeenTime);
        }
        clusterModel.setLaggingPartitionsMap(_newLaggingPartitionsMap);
        LOG.info("Lagging partitions map: {}  on thread after {}", _laggingPartitionsMap.toString(), Thread.currentThread().getName());
    }

    List<PartitionInfo> getLaggingPartitions() {
        return _laggingPartitions;
    }

    @Override
    protected void rebalanceForBroker(Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals,
            OptimizationOptions optimizationOptions) throws OptimizationFailureException {
        
        LOG.info("Current lagging partitions: {} ", _laggingPartitions.toString());
        if (_laggingRecoveryNeeded) {
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>(); 
            // gather all the lagging partitions into a reassignments map
            for (PartitionInfo laggingPartition: _laggingPartitions) {
                List<Node> laggingReplicas = new LinkedList<Node>(Arrays.asList(laggingPartition.replicas()));
                reassignments.put(new TopicPartition(laggingPartition.topic(), laggingPartition.partition()), 
                Optional.of(new NewPartitionReassignment(laggingReplicas.stream().map(node -> node.id()).collect(Collectors.toList()))));
                _laggingPartitionsMap.remove(new PartitionInfoWrapper(laggingPartition));
            }
            // use admin client to move them to same brokers
            try {
                _adminClient.alterPartitionReassignments(reassignments).all().get();
                reassignments.entrySet().stream().forEach(e -> LOG.info("Moved partition {}: {}", e.getKey(), e.getValue().get().targetReplicas()));
                _laggingPartitions.clear();
                _laggingRecoveryNeeded = false;
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Unable to move replicas onto same brokers");
            }
        }    
    }
}
