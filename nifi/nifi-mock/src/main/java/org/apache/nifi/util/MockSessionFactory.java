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
package org.apache.nifi.util;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;

public class MockSessionFactory implements ProcessSessionFactory {

    private final Processor processor;
    private final SharedSessionState sharedState;
    private final Set<MockProcessSession> createdSessions = new CopyOnWriteArraySet<>();

    MockSessionFactory(final SharedSessionState sharedState, final Processor processor) {
        this.sharedState = sharedState;
        this.processor = processor;
    }

    @Override
    public ProcessSession createSession() {
        final MockProcessSession session = new MockProcessSession(sharedState, processor);
        createdSessions.add(session);
        return session;
    }

    Set<MockProcessSession> getCreatedSessions() {
        return Collections.unmodifiableSet(createdSessions);
    }
}
