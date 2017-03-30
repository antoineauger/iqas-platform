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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestValidateCsv {

    @Test
    public void testHeaderAndSplit() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "true");
        runner.setProperty(ValidateCsv.VALIDATION_STRATEGY, ValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("Name,Birthdate,Weight\nJohn,22/11/1954,63.2\nBob,01/03/2004,45.0");
        runner.run();

        runner.assertTransferCount(ValidateCsv.REL_VALID, 1);
        runner.getFlowFilesForRelationship(ValidateCsv.REL_VALID).get(0).assertContentEquals("Name,Birthdate,Weight\nJohn,22/11/1954,63.2\nBob,01/03/2004,45.0");
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 0);

        runner.clearTransferState();

        runner.enqueue("Name,Birthdate,Weight\nJohn,22/11/1954,63a2\nBob,01/032004,45.0");
        runner.run();

        runner.assertTransferCount(ValidateCsv.REL_VALID, 0);
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
        runner.getFlowFilesForRelationship(ValidateCsv.REL_INVALID).get(0).assertContentEquals("Name,Birthdate,Weight\nJohn,22/11/1954,63a2\nBob,01/032004,45.0");

        runner.clearTransferState();

        runner.enqueue("Name,Birthdate,Weight\nJohn,22/111954,63.2\nBob,01/03/2004,45.0");
        runner.run();

        runner.assertTransferCount(ValidateCsv.REL_VALID, 1);
        runner.getFlowFilesForRelationship(ValidateCsv.REL_VALID).get(0).assertContentEquals("Name,Birthdate,Weight\nBob,01/03/2004,45.0");
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
        runner.getFlowFilesForRelationship(ValidateCsv.REL_INVALID).get(0).assertContentEquals("Name,Birthdate,Weight\nJohn,22/111954,63.2");
    }

    @Test
    public void testUniqueWithSplit() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");
        runner.setProperty(ValidateCsv.VALIDATION_STRATEGY, ValidateCsv.VALIDATE_LINES_INDIVIDUALLY);

        runner.setProperty(ValidateCsv.SCHEMA, "Unique()");

        runner.enqueue("John\r\nBob\r\nBob\r\nJohn");
        runner.run();

        runner.assertTransferCount(ValidateCsv.REL_VALID, 1);
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);

        runner.getFlowFilesForRelationship(ValidateCsv.REL_VALID).get(0).assertContentEquals("John\r\nBob");
        runner.getFlowFilesForRelationship(ValidateCsv.REL_INVALID).get(0).assertContentEquals("Bob\r\nJohn");
    }

    @Test
    public void testValidDateOptionalDouble() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        runner.setProperty(ValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), Optional(ParseDouble())");

        runner.enqueue("John,22/11/1954,63.2\r\nBob,01/03/2004,45.0");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("John,22/111954,63.2\r\nBob,01/03/2004,45.0");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testIsIncludedIn() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        runner.setProperty(ValidateCsv.SCHEMA, "Null, ParseDate(\"dd/MM/yyyy\"), IsIncludedIn(\"male\", \"female\")");

        runner.enqueue("John,22/11/1954,male\r\nMarie,01/03/2004,female");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("John,22/111954,63.2\r\nBob,01/03/2004,45.0");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testBigDecimalBoolCharIntLong() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "true");

        runner.setProperty(ValidateCsv.SCHEMA, "ParseBigDecimal(), ParseBool(), ParseChar(), ParseInt(), ParseLong()");

        runner.enqueue("bigdecimal,bool,char,integer,long\r\n10.0001,true,c,1,92147483647");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("bigdecimal,bool,char,integer,long\r\n10.0001,true,c,92147483647,92147483647");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testEqualsNotNullStrNotNullOrEmpty() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        runner.setProperty(ValidateCsv.SCHEMA, "Equals(), NotNull(), StrNotNullOrEmpty()");

        runner.enqueue("test,test,test\r\ntest,test,test");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("test,test,test\r\ntset,test,test");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testStrlenStrMinMaxStrRegex() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        runner.setProperty(ValidateCsv.SCHEMA, "Strlen(4), StrMinMax(3,5), StrRegex(\"[a-z0-9\\._]+@[a-z0-9\\.]+\")");

        runner.enqueue("test,test,test@apache.org");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("test,test,testapache.org");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testDMinMaxForbidSubStrLMinMax() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        runner.setProperty(ValidateCsv.SCHEMA, "DMinMax(10,100),LMinMax(10,100),ForbidSubStr(\"test\", \"tset\")");

        runner.enqueue("50.001,50,hello");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("10,10,testapache.org");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testUnique() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        runner.setProperty(ValidateCsv.SCHEMA, "Unique(), UniqueHashCode()");

        runner.enqueue("1,2\r\n3,4");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("1,2\r\n1,4");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testRequire() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        int hashcode = "test".hashCode();
        runner.setProperty(ValidateCsv.SCHEMA, "RequireHashCode(" + hashcode + "), RequireSubStr(\"test\")");

        runner.enqueue("test,test");
        runner.run();
        runner.assertAllFlowFilesTransferred(ValidateCsv.REL_VALID, 1);

        runner.enqueue("tset,tset");
        runner.run();
        runner.assertTransferCount(ValidateCsv.REL_INVALID, 1);
    }

    @Test
    public void testValidate() {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateCsv());
        runner.setProperty(ValidateCsv.DELIMITER_CHARACTER, ",");
        runner.setProperty(ValidateCsv.END_OF_LINE_CHARACTER, "\r\n");
        runner.setProperty(ValidateCsv.QUOTE_CHARACTER, "\"");
        runner.setProperty(ValidateCsv.HEADER, "false");

        runner.setProperty(ValidateCsv.SCHEMA, "RequireSubString(\"test\")");
        runner.assertNotValid();

        runner.setProperty(ValidateCsv.SCHEMA, "''");
        runner.assertNotValid();

        runner.setProperty(ValidateCsv.SCHEMA, "\"\"");
        runner.assertNotValid();
    }

}
