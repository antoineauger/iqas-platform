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
package org.apache.nifi.reporting.ambari.metrics;

import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A service used to produce key/value metrics based on a given input.
 */
public class MetricsService {

    /**
     * Generates a Map of metrics for a ProcessGroupStatus instance.
     *
     * @param status a ProcessGroupStatus to get metrics from
     * @return a map of metrics for the given status
     */
    public Map<String,String> getMetrics(ProcessGroupStatus status) {
        final Map<String,String> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, String.valueOf(status.getFlowFilesReceived()));
        metrics.put(MetricNames.BYTES_RECEIVED, String.valueOf(status.getBytesReceived()));
        metrics.put(MetricNames.FLOW_FILES_SENT, String.valueOf(status.getFlowFilesSent()));
        metrics.put(MetricNames.BYTES_SENT, String.valueOf(status.getBytesSent()));
        metrics.put(MetricNames.FLOW_FILES_QUEUED, String.valueOf(status.getQueuedCount()));
        metrics.put(MetricNames.BYTES_QUEUED, String.valueOf(status.getQueuedContentSize()));
        metrics.put(MetricNames.BYTES_READ, String.valueOf(status.getBytesRead()));
        metrics.put(MetricNames.BYTES_WRITTEN, String.valueOf(status.getBytesWritten()));
        metrics.put(MetricNames.ACTIVE_THREADS, String.valueOf(status.getActiveThreadCount()));

        final long durationNanos = calculateProcessingNanos(status);
        metrics.put(MetricNames.TOTAL_TASK_DURATION_NANOS, String.valueOf(durationNanos));

        final long durationSeconds = TimeUnit.SECONDS.convert(durationNanos, TimeUnit.NANOSECONDS);
        metrics.put(MetricNames.TOTAL_TASK_DURATION_SECONDS, String.valueOf(durationSeconds));

        return metrics;
    }

    /**
     * Generates a Map of metrics for VirtualMachineMetrics.
     *
     * @param virtualMachineMetrics a VirtualMachineMetrics instance to get metrics from
     * @return a map of metrics from the given VirtualMachineStatus
     */
    public Map<String,String> getMetrics(VirtualMachineMetrics virtualMachineMetrics) {
        final Map<String,String> metrics = new HashMap<>();
        metrics.put(MetricNames.JVM_UPTIME, String.valueOf(virtualMachineMetrics.uptime()));
        metrics.put(MetricNames.JVM_HEAP_USED, String.valueOf(virtualMachineMetrics.heapUsed()));
        metrics.put(MetricNames.JVM_HEAP_USAGE, String.valueOf(virtualMachineMetrics.heapUsage()));
        metrics.put(MetricNames.JVM_NON_HEAP_USAGE, String.valueOf(virtualMachineMetrics.nonHeapUsage()));
        metrics.put(MetricNames.JVM_THREAD_COUNT, String.valueOf(virtualMachineMetrics.threadCount()));
        metrics.put(MetricNames.JVM_DAEMON_THREAD_COUNT, String.valueOf(virtualMachineMetrics.daemonThreadCount()));
        metrics.put(MetricNames.JVM_FILE_DESCRIPTOR_USAGE, String.valueOf(virtualMachineMetrics.fileDescriptorUsage()));

        for (Map.Entry<Thread.State,Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
            final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
            switch(entry.getKey()) {
                case BLOCKED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_BLOCKED, String.valueOf(normalizedValue));
                    break;
                case RUNNABLE:
                    metrics.put(MetricNames.JVM_THREAD_STATES_RUNNABLE, String.valueOf(normalizedValue));
                    break;
                case TERMINATED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TERMINATED, String.valueOf(normalizedValue));
                    break;
                case TIMED_WAITING:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TIMED_WAITING, String.valueOf(normalizedValue));
                    break;
                default:
                    break;
            }
        }

        for (Map.Entry<String,VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final String gcName = entry.getKey().replace(" ", "");
            final long runs = entry.getValue().getRuns();
            final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
            metrics.put(MetricNames.JVM_GC_RUNS + "." + gcName, String.valueOf(runs));
            metrics.put(MetricNames.JVM_GC_TIME + "." + gcName, String.valueOf(timeMS));
        }

        return metrics;
    }

    // calculates the total processing time of all processors in nanos
    protected long calculateProcessingNanos(final ProcessGroupStatus status) {
        long nanos = 0L;

        for (final ProcessorStatus procStats : status.getProcessorStatus()) {
            nanos += procStats.getProcessingNanos();
        }

        for (final ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {
            nanos += calculateProcessingNanos(childGroupStatus);
        }

        return nanos;
    }

}
