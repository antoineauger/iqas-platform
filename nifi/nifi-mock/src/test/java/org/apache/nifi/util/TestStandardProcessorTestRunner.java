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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Test;

public class TestStandardProcessorTestRunner {

    @Test
    public void testProcessContextPassedToOnStoppedMethods() {
        final ProcessorWithOnStop proc = new ProcessorWithOnStop();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        assertEquals(0, proc.getOnStoppedCallsWithContext());
        assertEquals(0, proc.getOnStoppedCallsWithoutContext());

        runner.run(1, false);

        assertEquals(0, proc.getOnStoppedCallsWithContext());
        assertEquals(0, proc.getOnStoppedCallsWithoutContext());

        runner.run(1, true);

        assertEquals(1, proc.getOnStoppedCallsWithContext());
        assertEquals(1, proc.getOnStoppedCallsWithoutContext());
    }

    @Test
    public void testNumThreads() {
        final ProcessorWithOnStop proc = new ProcessorWithOnStop();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setThreadCount(5);
        runner.run(1, true);
        assertEquals(5, runner.getProcessContext().getMaxConcurrentTasks());
    }

    @Test
    public void testFlowFileValidator() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(5, true);
        runner.assertTransferCount(AddAttributeProcessor.REL_SUCCESS, 3);
        runner.assertTransferCount(AddAttributeProcessor.REL_FAILURE, 2);
        runner.assertAllFlowFilesContainAttribute(AddAttributeProcessor.REL_SUCCESS, AddAttributeProcessor.KEY);
        runner.assertAllFlowFiles(AddAttributeProcessor.REL_SUCCESS, new FlowFileValidator() {
            @Override
            public void assertFlowFile(FlowFile f) {
                assertEquals("value", f.getAttribute(AddAttributeProcessor.KEY));
            }
        });
    }

    @Test(expected = AssertionError.class)
    public void testFailFlowFileValidator() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(5, true);
        runner.assertAllFlowFiles(new FlowFileValidator() {
            @Override
            public void assertFlowFile(FlowFile f) {
                assertEquals("value", f.getAttribute(AddAttributeProcessor.KEY));
            }
        });
    }

    @Test(expected = AssertionError.class)
    public void testFailAllFlowFilesContainAttribute() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(5, true);
        runner.assertAllFlowFilesContainAttribute(AddAttributeProcessor.KEY);
    }

    @Test
    public void testAllFlowFilesContainAttribute() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(1, true);
        runner.assertAllFlowFilesContainAttribute(AddAttributeProcessor.KEY);
    }

    private static class ProcessorWithOnStop extends AbstractProcessor {

        private int callsWithContext = 0;
        private int callsWithoutContext = 0;

        @OnStopped
        public void onStoppedWithContext(final ProcessContext procContext) {
            callsWithContext++;
        }

        @OnStopped
        public void onStoppedWithoutContext() {
            callsWithoutContext++;
        }

        public int getOnStoppedCallsWithContext() {
            return callsWithContext;
        }

        public int getOnStoppedCallsWithoutContext() {
            return callsWithoutContext;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }

    }

    private static class AddAttributeProcessor extends AbstractProcessor {
        public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("success").build();
        public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("failure").build();
        public static final String KEY = "KEY";

        private Set<Relationship> relationships;
        private int counter = 0;

        @Override
        protected void init(final ProcessorInitializationContext context) {
            final Set<Relationship> relationships = new HashSet<>();
            relationships.add(REL_SUCCESS);
            relationships.add(REL_FAILURE);
            this.relationships = Collections.unmodifiableSet(relationships);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            FlowFile ff = session.create();
            if(counter % 2 == 0) {
                ff = session.putAttribute(ff, KEY, "value");
                session.transfer(ff, REL_SUCCESS);
            } else {
                session.transfer(ff, REL_FAILURE);
            }
            counter++;
        }
    }
}
