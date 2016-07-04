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
package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.repository.RingBufferEventRepository;
import org.apache.nifi.controller.repository.StandardRepositoryStatusReport;
import org.apache.nifi.controller.repository.FlowFileEvent;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TestRingBufferEventRepository {

    @Test
    public void testAdd() throws IOException {
        final RingBufferEventRepository repo = new RingBufferEventRepository(5);
        long insertNanos = 0L;
        for (int i = 0; i < 1000000; i++) {
            final FlowFileEvent event = generateEvent();

            final long insertStart = System.nanoTime();
            repo.updateRepository(event);
            insertNanos += System.nanoTime() - insertStart;
        }

        final long queryStart = System.nanoTime();
        final StandardRepositoryStatusReport report = repo.reportTransferEvents(System.currentTimeMillis() - 2 * 60000);
        final long queryNanos = System.nanoTime() - queryStart;
        System.out.println(report);
        System.out.println("Insert: " + TimeUnit.MILLISECONDS.convert(insertNanos, TimeUnit.NANOSECONDS));
        System.out.println("Query: " + TimeUnit.MILLISECONDS.convert(queryNanos, TimeUnit.NANOSECONDS));
        repo.close();
    }

    private FlowFileEvent generateEvent() {
        return new FlowFileEvent() {
            @Override
            public String getComponentIdentifier() {
                return "ABC";
            }

            @Override
            public int getFlowFilesIn() {
                return 1;
            }

            @Override
            public int getFlowFilesOut() {
                return 1;
            }

            @Override
            public long getContentSizeIn() {
                return 1024L;
            }

            @Override
            public long getContentSizeOut() {
                return 1024 * 1024L;
            }

            @Override
            public long getBytesRead() {
                return 1024L;
            }

            @Override
            public long getBytesWritten() {
                return 1024L * 1024L;
            }

            @Override
            public long getContentSizeRemoved() {
                return 1024;
            }

            @Override
            public int getFlowFilesRemoved() {
                return 1;
            }

            @Override
            public long getProcessingNanoseconds() {
                return 234782;
            }

            @Override
            public int getInvocations() {
                return 1;
            }

            @Override
            public long getAggregateLineageMillis() {
                return 783L;
            }

            @Override
            public long getAverageLineageMillis() {
                return getAggregateLineageMillis() / (getFlowFilesRemoved() + getFlowFilesOut());
            }

            @Override
            public int getFlowFilesReceived() {
                return 0;
            }

            @Override
            public long getBytesReceived() {
                return 0;
            }

            @Override
            public int getFlowFilesSent() {
                return 0;
            }

            @Override
            public long getBytesSent() {
                return 0;
            }
        };
    }
}
