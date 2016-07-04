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
package org.apache.nifi.processors.couchbase;

import static org.apache.nifi.couchbase.CouchbaseAttributes.Exception;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.BUCKET_NAME;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.COUCHBASE_CLUSTER_SERVICE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOCUMENT_TYPE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.DOC_ID;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_FAILURE;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_ORIGINAL;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_RETRY;
import static org.apache.nifi.processors.couchbase.AbstractCouchbaseProcessor.REL_SUCCESS;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.apache.nifi.couchbase.CouchbaseAttributes;
import org.apache.nifi.couchbase.CouchbaseClusterControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.core.ServiceNotAvailableException;
import com.couchbase.client.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.error.DurabilityException;
import com.couchbase.client.java.error.RequestTooBigException;


public class TestGetCouchbaseKey {

    private static final String SERVICE_ID = "couchbaseClusterService";
    private TestRunner testRunner;

    @Before
    public void init() throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.GetCouchbaseKey", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.couchbase.TestGetCouchbaseKey", "debug");

        testRunner = TestRunners.newTestRunner(GetCouchbaseKey.class);
        testRunner.setValidateExpressionUsage(false);
    }

    private void setupMockBucket(Bucket bucket) throws InitializationException {
        CouchbaseClusterControllerService service = mock(CouchbaseClusterControllerService.class);
        when(service.getIdentifier()).thenReturn(SERVICE_ID);
        when(service.openBucket(anyString())).thenReturn(bucket);
        testRunner.addControllerService(SERVICE_ID, service);
        testRunner.enableControllerService(service);
        testRunner.setProperty(COUCHBASE_CLUSTER_SERVICE, SERVICE_ID);
    }

    @Test
    public void testStaticDocId() throws Exception {
        String bucketName = "bucket-1";
        String docId = "doc-a";

        Bucket bucket = mock(Bucket.class);
        String content = "{\"key\":\"value\"}";
        int expiry = 100;
        long cas = 200L;
        when(bucket.get(docId, RawJsonDocument.class)).thenReturn(RawJsonDocument.create(docId, expiry, content, cas));
        setupMockBucket(bucket);

        testRunner.setProperty(BUCKET_NAME, bucketName);
        testRunner.setProperty(DOC_ID, docId);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);

        outFile.assertAttributeEquals(CouchbaseAttributes.Cluster.key(), SERVICE_ID);
        outFile.assertAttributeEquals(CouchbaseAttributes.Bucket.key(), bucketName);
        outFile.assertAttributeEquals(CouchbaseAttributes.DocId.key(), docId);
        outFile.assertAttributeEquals(CouchbaseAttributes.Cas.key(), String.valueOf(cas));
        outFile.assertAttributeEquals(CouchbaseAttributes.Expiry.key(), String.valueOf(expiry));
    }


    @Test
    public void testDocIdExp() throws Exception {
        String docIdExp = "${'someProperty'}";
        String somePropertyValue = "doc-p";

        Bucket bucket = mock(Bucket.class);
        String content = "{\"key\":\"value\"}";
        when(bucket.get(somePropertyValue, RawJsonDocument.class))
            .thenReturn(RawJsonDocument.create(somePropertyValue, content));
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        byte[] inFileData = "input FlowFile data".getBytes(StandardCharsets.UTF_8);
        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", somePropertyValue);
        testRunner.enqueue(inFileData, properties);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
    }

    @Test
    public void testDocIdExpWithEmptyFlowFile() throws Exception {
        String docIdExp = "doc-s";
        String docId = "doc-s";

        Bucket bucket = mock(Bucket.class);
        String content = "{\"key\":\"value\"}";
        when(bucket.get(docId, RawJsonDocument.class))
            .thenReturn(RawJsonDocument.create(docId, content));
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
    }

    @Test
    public void testDocIdExpWithInvalidExpression() throws Exception {
        String docIdExp = "${nonExistingFunction('doc-s')}";
        String docId = "doc-s";

        Bucket bucket = mock(Bucket.class);
        String content = "{\"key\":\"value\"}";
        when(bucket.get(docId, RawJsonDocument.class))
            .thenReturn(RawJsonDocument.create(docId, content));
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);
        testRunner.enqueue(new byte[0]);

        try {
            testRunner.run();
            fail("Exception should be thrown.");
        } catch (AssertionError e) {
            Assert.assertTrue(e.getCause().getClass().equals(AttributeExpressionLanguageException.class));
        }

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testDocIdExpWithInvalidExpressionOnFlowFile() throws Exception {
        String docIdExp = "${nonExistingFunction(someProperty)}";

        Bucket bucket = mock(Bucket.class);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        Map<String, String> properties = new HashMap<>();
        properties.put("someProperty", "someValue");
        testRunner.enqueue(inFileData, properties);
        try {
            testRunner.run();
            fail("Exception should be thrown.");
        } catch (AssertionError e) {
            Assert.assertTrue(e.getCause().getClass().equals(AttributeExpressionLanguageException.class));
        }

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testInputFlowFileContent() throws Exception {

        Bucket bucket = mock(Bucket.class);
        String inFileDataStr = "doc-in";
        String content = "{\"key\":\"value\"}";
        when(bucket.get(inFileDataStr, RawJsonDocument.class))
            .thenReturn(RawJsonDocument.create(inFileDataStr, content));
        setupMockBucket(bucket);


        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_ORIGINAL).get(0);
        orgFile.assertContentEquals(inFileDataStr);
    }

    @Test
    public void testBinaryDocument() throws Exception {

        Bucket bucket = mock(Bucket.class);
        String inFileDataStr = "doc-in";
        String content = "binary";
        ByteBuf buf = Unpooled.copiedBuffer(content.getBytes(StandardCharsets.UTF_8));
        when(bucket.get(inFileDataStr, BinaryDocument.class))
            .thenReturn(BinaryDocument.create(inFileDataStr, buf));
        setupMockBucket(bucket);


        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.setProperty(DOCUMENT_TYPE, DocumentType.Binary.toString());
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 1);
        testRunner.assertTransferCount(REL_ORIGINAL, 1);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile outFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_ORIGINAL).get(0);
        orgFile.assertContentEquals(inFileDataStr);
    }


    @Test
    public void testCouchbaseFailure() throws Exception {

        Bucket bucket = mock(Bucket.class);
        String inFileDataStr = "doc-in";
        when(bucket.get(inFileDataStr, RawJsonDocument.class))
            .thenThrow(new ServiceNotAvailableException());
        setupMockBucket(bucket);


        byte[] inFileData = inFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        try {
            testRunner.run();
            fail("ProcessException should be thrown.");
        } catch (AssertionError e) {
            Assert.assertTrue(e.getCause().getClass().equals(ProcessException.class));
        }

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseConfigurationError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        when(bucket.get(docIdExp, RawJsonDocument.class))
            .thenThrow(new AuthenticationException());
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        try {
            testRunner.run();
            fail("ProcessException should be thrown.");
        } catch (AssertionError e) {
            Assert.assertTrue(e.getCause().getClass().equals(ProcessException.class));
            Assert.assertTrue(e.getCause().getCause().getClass().equals(AuthenticationException.class));
        }

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 0);
    }

    @Test
    public void testCouchbaseInvalidInputError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        CouchbaseException exception = new RequestTooBigException();
        when(bucket.get(docIdExp, RawJsonDocument.class))
            .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 1);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }

    @Test
    public void testCouchbaseTempClusterError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        CouchbaseException exception = new BackpressureException();
        when(bucket.get(docIdExp, RawJsonDocument.class))
            .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }


    @Test
    public void testCouchbaseTempFlowFileError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        // There is no suitable CouchbaseException for temp flowfile error, currently.
        CouchbaseException exception = new DurabilityException();
        when(bucket.get(docIdExp, RawJsonDocument.class))
            .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }

    @Test
    public void testCouchbaseFatalError() throws Exception {
        String docIdExp = "doc-c";

        Bucket bucket = mock(Bucket.class);
        CouchbaseException exception = new NotConnectedException();
        when(bucket.get(docIdExp, RawJsonDocument.class))
            .thenThrow(exception);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 1);
        testRunner.assertTransferCount(REL_FAILURE, 0);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_RETRY).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), exception.getClass().getName());
    }

    @Test
    public void testDocumentNotFound() throws Exception {
        String docIdExp = "doc-n";

        Bucket bucket = mock(Bucket.class);
        when(bucket.get(docIdExp, RawJsonDocument.class))
            .thenReturn(null);
        setupMockBucket(bucket);

        testRunner.setProperty(DOC_ID, docIdExp);

        String inputFileDataStr = "input FlowFile data";
        byte[] inFileData = inputFileDataStr.getBytes(StandardCharsets.UTF_8);
        testRunner.enqueue(inFileData);
        testRunner.run();

        testRunner.assertTransferCount(REL_SUCCESS, 0);
        testRunner.assertTransferCount(REL_ORIGINAL, 0);
        testRunner.assertTransferCount(REL_RETRY, 0);
        testRunner.assertTransferCount(REL_FAILURE, 1);
        MockFlowFile orgFile = testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        orgFile.assertContentEquals(inputFileDataStr);
        orgFile.assertAttributeEquals(Exception.key(), DocumentDoesNotExistException.class.getName());
    }
}
