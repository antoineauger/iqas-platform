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

package org.apache.nifi.processors.kafka.pubsub;

import java.util.Collection;
import java.util.Collections;

import org.apache.nifi.flowfile.FlowFile;

public interface PublishResult {
    Collection<FlowFile> getSuccessfulFlowFiles();

    Collection<FlowFile> getFailedFlowFiles();

    int getSuccessfulMessageCount(FlowFile flowFile);

    Exception getReasonForFailure(FlowFile flowFile);


    public static final PublishResult EMPTY = new PublishResult() {
        @Override
        public Collection<FlowFile> getSuccessfulFlowFiles() {
            return Collections.emptyList();
        }

        @Override
        public Collection<FlowFile> getFailedFlowFiles() {
            return Collections.emptyList();
        }

        @Override
        public int getSuccessfulMessageCount(FlowFile flowFile) {
            return 0;
        }

        @Override
        public Exception getReasonForFailure(FlowFile flowFile) {
            return null;
        }
    };
}
