/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 * adding due to checkstyle 
 */
package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal.ClusterModelStatsComparator;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LaggingReplicaReassignmentGoal.PartitionInfoWrapper;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.easymock.EasyMock;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig.MAX_LAGGING_REPLICA_REASSIGN_MS;
import static com.linkedin.kafka.cruisecontrol.model.SortedReplicasTest.generateBroker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaCruiseControlUtils.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LaggingReplicaReassignmentGoalTest {
    
    /**
     * @throws Exception 
     * 
    */
    @Test
    public void checkifLaggingPartitionReplicaMoved() throws Exception {
        LaggingReplicaReassignmentGoal goal = new LaggingReplicaReassignmentGoal();
        Map props = getKafkaCruiseControlProperties();
        Map<String, Object> configs = (Map<String, Object>) props;
        configs.put(MAX_LAGGING_REPLICA_REASSIGN_MS, 1000L);
        System.out.println(configs);
        
        //mock admin client
        AdminClient adminClient = EasyMock.createMock(AdminClient.class);
        PowerMock.mockStatic(KafkaCruiseControlUtils.class);
        EasyMock.expect(KafkaCruiseControlUtils.createAdminClient(EasyMock.anyObject())).andReturn(adminClient);
        EasyMock.expect(KafkaCruiseControlUtils.parseAdminClientConfigs(EasyMock.anyObject())).andReturn(configs);
        PowerMock.replay(KafkaCruiseControlUtils.class);
        
        //mock alterPartitionReassignment which should happen only once
        AlterPartitionReassignmentsResult aprResult = EasyMock.createMock(AlterPartitionReassignmentsResult.class);
        EasyMock.expect(aprResult.all()).andReturn(KafkaFuture.completedFuture(null)).times(1);

        goal.configure(configs);
        Set<Goal> optimizedGoals = new HashSet<Goal>();
        optimizedGoals.add(goal);
        assertEquals(goal.name(), LaggingReplicaReassignmentGoal.class.getSimpleName());

        ClusterModel clusterModel = EasyMock.createMock(ClusterModel.class);
        ClusterModelStats clusterModelStats = EasyMock.createMock(ClusterModelStats.class);
        ClusterModelStatsComparator clusterModelStatsComparator = EasyMock.createMock(ClusterModelStatsComparator.class);

        Node node1 = new Node(0, "node1", 9092);
        Node node2 = new Node(1, "node2", 9092);
        Node node3 = new Node(2, "node3", 9092);
        Node[] replicas = {node1, node2, node3};
        Node[] isr = {node1, node2};
        PartitionInfo partitionInfo1 = new PartitionInfo("topic1", 0, node1, replicas, isr);
        PartitionInfo partitionInfo2 = new PartitionInfo("topic2", 0, node1, replicas, isr);
        PartitionInfo partitionInfo2Copy = new PartitionInfo("topic2", 0, node1, replicas, isr);
        List<PartitionInfo> partitionsWithLaggingReplicas1 = new ArrayList<PartitionInfo>();
        partitionsWithLaggingReplicas1.add(partitionInfo1);
        List<PartitionInfo> partitionsWithLaggingReplicas2 = new ArrayList<PartitionInfo>();
        partitionsWithLaggingReplicas2.add(partitionInfo2);
        List<PartitionInfo> partitionsWithLaggingReplicas12 = new ArrayList<PartitionInfo>(partitionsWithLaggingReplicas1);
        partitionsWithLaggingReplicas12.add(partitionInfo2);
        // send 1 partition w/ lagging replica
        EasyMock.expect(clusterModelStats.numPartitionsWithLaggingReplicas()).andReturn(1).times(6);
        EasyMock.expect(clusterModel.getClusterStats(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(clusterModelStats).anyTimes();
        EasyMock.expect(clusterModelStatsComparator.compare(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(0);

        EasyMock.expectLastCall().andAnswer(() -> {
            return null;
        }).anyTimes();
        clusterModel.clearSortedReplicas();
        clusterModel.clearSortedReplicas();
        EasyMock.expect(clusterModel.getPartitionsWithLaggingReplicas()).andReturn(new ArrayList<PartitionInfo>());
        EasyMock.expect(clusterModel.getPartitionsWithLaggingReplicas()).andReturn(partitionsWithLaggingReplicas1);
        EasyMock.expect(clusterModel.getPartitionsWithLaggingReplicas()).andReturn(partitionsWithLaggingReplicas12);
        EasyMock.expect(clusterModel.getPartitionsWithLaggingReplicas()).andReturn(partitionsWithLaggingReplicas2);
        EasyMock.expect(clusterModel.brokenBrokers()).andReturn(new TreeSet<Broker>()).anyTimes();
        EasyMock.expect(clusterModel.brokers()).andReturn(new TreeSet<Broker>(Arrays.asList(replicas).stream()
                        .map(node -> generateBroker(node.id(), 0)).collect(Collectors.toList()))).anyTimes();
        EasyMock.expect(clusterModel.aliveBrokers()).andReturn(new TreeSet<Broker>()).anyTimes();
        EasyMock.expect(adminClient.alterPartitionReassignments(EasyMock.anyObject())).andReturn(aprResult);
        EasyMock.replay(clusterModel, clusterModelStats, clusterModelStatsComparator, adminClient, aprResult);
        // optimize once before sleep 
        goal.optimize(clusterModel, optimizedGoals, new OptimizationOptions(new HashSet<String>(), new HashSet<Integer>(), new HashSet<Integer>()));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // optimize again after sleep
        goal.optimize(clusterModel, optimizedGoals, new OptimizationOptions(new HashSet<String>(), new HashSet<Integer>(), new HashSet<Integer>()));
        // should have been moved as 1st seen before sleep
        assertFalse(goal._laggingPartitionsMap.containsKey(new PartitionInfoWrapper(partitionInfo1)));
        // still hasnt reached maxLagTimeMS threshold
        assertTrue(goal._laggingPartitionsMap.containsKey(new PartitionInfoWrapper(partitionInfo2Copy)));
        EasyMock.verify(clusterModel);
    }

    /** 
     * Start with both tp's lagging, then both have all in-sync, then they start lagging again.
     * In this case, they should be absent (from target replicas to be moved) 
     * post 2nd-optimize and present post 3rd-optimize
    */
    @Test
    public void checkNonLaggingPartitionNotMoved() throws OptimizationFailureException {
        LaggingReplicaReassignmentGoal goal = new LaggingReplicaReassignmentGoal();
        Map props = getKafkaCruiseControlProperties();
        Map<String, Object> configs = (Map<String, Object>) props;
        configs.put(MAX_LAGGING_REPLICA_REASSIGN_MS, 1000L);

        //mock admin client
        AdminClient adminClient = EasyMock.createMock(AdminClient.class);
        PowerMock.mockStatic(KafkaCruiseControlUtils.class);
        EasyMock.expect(KafkaCruiseControlUtils.createAdminClient(EasyMock.anyObject())).andReturn(adminClient);
        EasyMock.expect(KafkaCruiseControlUtils.parseAdminClientConfigs(EasyMock.anyObject())).andReturn(configs);
        PowerMock.replay(KafkaCruiseControlUtils.class);

        goal.configure(configs);
        Set<Goal> optimizedGoals = new HashSet<Goal>();
        optimizedGoals.add(goal);

        ClusterModel clusterModel = EasyMock.createMock(ClusterModel.class);
        ClusterModelStats clusterModelStats = EasyMock.createMock(ClusterModelStats.class);
        ClusterModelStatsComparator clusterModelStatsComparator = EasyMock.createMock(ClusterModelStatsComparator.class);

        Node node1 = new Node(0, "node1", 9092);
        Node node2 = new Node(1, "node2", 9092);
        Node node3 = new Node(2, "node3", 9092);
        Node[] replicas = {node1, node2, node3};
        Node[] isr = {node1, node2};
        PartitionInfo partitionInfo1 = new PartitionInfo("topic1", 0, node1, replicas, isr);
        PartitionInfo partitionInfo2 = new PartitionInfo("topic2", 0, node1, replicas, isr);
        PartitionInfo partitionInfo2Copy = new PartitionInfo("topic2", 0, node1, replicas, isr);
        List<PartitionInfo> partitionsWithLaggingReplicas12 = new ArrayList<PartitionInfo>();
        partitionsWithLaggingReplicas12.add(partitionInfo1);
        partitionsWithLaggingReplicas12.add(partitionInfo2);
        // send 1 partition w/ lagging replica
        EasyMock.expect(clusterModelStats.numPartitionsWithLaggingReplicas()).andReturn(1).times(9);
        EasyMock.expect(clusterModel.getClusterStats(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(clusterModelStats).anyTimes();
        EasyMock.expect(clusterModelStatsComparator.compare(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(0);

        EasyMock.expectLastCall().andAnswer(() -> {
            return null;
        }).anyTimes();
        clusterModel.clearSortedReplicas();
        clusterModel.clearSortedReplicas();
        clusterModel.clearSortedReplicas();
        EasyMock.expect(clusterModel.getPartitionsWithLaggingReplicas()).andReturn(partitionsWithLaggingReplicas12).times(2);
        EasyMock.expect(clusterModel.getPartitionsWithLaggingReplicas()).andReturn(new ArrayList<PartitionInfo>()).times(2);
        EasyMock.expect(clusterModel.getPartitionsWithLaggingReplicas()).andReturn(partitionsWithLaggingReplicas12).times(2);
        EasyMock.expect(clusterModel.brokenBrokers()).andReturn(new TreeSet<Broker>()).anyTimes();
        EasyMock.expect(clusterModel.brokers()).andReturn(new TreeSet<Broker>(Arrays.asList(replicas).stream()
                        .map(node -> generateBroker(node.id(), 0)).collect(Collectors.toList()))).anyTimes();
        EasyMock.expect(clusterModel.aliveBrokers()).andReturn(new TreeSet<Broker>()).anyTimes();
        EasyMock.replay(clusterModel, clusterModelStats, clusterModelStatsComparator);
        // optimize once before sleep 
        goal.optimize(clusterModel, optimizedGoals, new OptimizationOptions(new HashSet<String>(), new HashSet<Integer>(), new HashSet<Integer>()));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // optimize again after sleep
        goal.optimize(clusterModel, optimizedGoals, new OptimizationOptions(new HashSet<String>(), new HashSet<Integer>(), new HashSet<Integer>()));
        assertFalse(goal._laggingPartitionsMap.containsKey(new PartitionInfoWrapper(partitionInfo1)));
        assertFalse(goal._laggingPartitionsMap.containsKey(new PartitionInfoWrapper(partitionInfo2)));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // optimize again after sleep
        goal.optimize(clusterModel, optimizedGoals, new OptimizationOptions(new HashSet<String>(), new HashSet<Integer>(), new HashSet<Integer>()));
        assertTrue(goal._laggingPartitionsMap.containsKey(new PartitionInfoWrapper(partitionInfo1)));
        assertTrue(goal._laggingPartitionsMap.containsKey(new PartitionInfoWrapper(partitionInfo2Copy)));
        EasyMock.verify(clusterModel);
    }
}
