/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

public class PartitionInfoWrapper {
    PartitionInfo _pi;

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
                && _pi.leader().id() == p2.leader().id() && _pi.inSyncReplicas().length == p2.inSyncReplicas().length) {
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
