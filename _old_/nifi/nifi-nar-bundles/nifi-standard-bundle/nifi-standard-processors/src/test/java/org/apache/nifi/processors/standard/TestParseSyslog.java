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

package org.apache.nifi.processors.standard;

import org.apache.nifi.processors.standard.syslog.SyslogAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestParseSyslog {
    static final String PRI = "34";
    static final String SEV = "2";
    static final String FAC = "4";
    static final String TIME = "Oct 13 15:43:23";
    static final String HOST = "localhost.home";
    static final String BODY = "some message";

    static final String VALID_MESSAGE_RFC3164_0 = "<" + PRI + ">" + TIME + " " + HOST + " " + BODY + "\n";

    @Test
    public void testSuccessfulParse3164() {
        final TestRunner runner = TestRunners.newTestRunner(new ParseSyslog());
        runner.enqueue(VALID_MESSAGE_RFC3164_0.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseSyslog.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseSyslog.REL_SUCCESS).get(0);
        mff.assertAttributeEquals(SyslogAttributes.BODY.key(), BODY);
        mff.assertAttributeEquals(SyslogAttributes.FACILITY.key(), FAC);
        mff.assertAttributeEquals(SyslogAttributes.HOSTNAME.key(), HOST);
        mff.assertAttributeEquals(SyslogAttributes.PRIORITY.key(), PRI);
        mff.assertAttributeEquals(SyslogAttributes.SEVERITY.key(), SEV);
        mff.assertAttributeEquals(SyslogAttributes.TIMESTAMP.key(), TIME);
    }


    @Test
    public void testInvalidMessage() {
        final TestRunner runner = TestRunners.newTestRunner(new ParseSyslog());
        runner.enqueue("<hello> yesterday localhost\n".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseSyslog.REL_FAILURE, 1);
    }
}
