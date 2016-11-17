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
package org.apache.nifi.processors.solr;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for PutSolr processor.
 */
public class TestPutSolrContentStream {

    static final String DEFAULT_SOLR_CORE = "testCollection";

    static final String CUSTOM_JSON_SINGLE_DOC_FILE = "src/test/resources/testdata/test-custom-json-single-doc.json";
    static final String SOLR_JSON_MULTIPLE_DOCS_FILE = "src/test/resources/testdata/test-solr-json-multiple-docs.json";
    static final String CSV_MULTIPLE_DOCS_FILE = "src/test/resources/testdata/test-csv-multiple-docs.csv";
    static final String XML_MULTIPLE_DOCS_FILE = "src/test/resources/testdata/test-xml-multiple-docs.xml";

    static final SolrDocument expectedDoc1 = new SolrDocument();
    static {
        expectedDoc1.addField("first", "John");
        expectedDoc1.addField("last", "Doe");
        expectedDoc1.addField("grade", 8);
        expectedDoc1.addField("subject", "Math");
        expectedDoc1.addField("test", "term1");
        expectedDoc1.addField("marks", 90);
    }

    static final SolrDocument expectedDoc2 = new SolrDocument();
    static {
        expectedDoc2.addField("first", "John");
        expectedDoc2.addField("last", "Doe");
        expectedDoc2.addField("grade", 8);
        expectedDoc2.addField("subject", "Biology");
        expectedDoc2.addField("test", "term1");
        expectedDoc2.addField("marks", 86);
    }

    /**
     * Creates a base TestRunner with Solr Type of standard.
     */
    private static TestRunner createDefaultTestRunner(PutSolrContentStream processor) {
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        return runner;
    }

    @Test
    public void testUpdateWithSolrJson() throws IOException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        final TestableProcessor proc = new TestableProcessor(solrClient);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update/json/docs");
        runner.setProperty("json.command", "false");

        try (FileInputStream fileIn = new FileInputStream(SOLR_JSON_MULTIPLE_DOCS_FILE)) {
            runner.enqueue(fileIn);

            runner.run(1, false);
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testUpdateWithCustomJson() throws IOException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        final TestableProcessor proc = new TestableProcessor(solrClient);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update/json/docs");
        runner.setProperty("split", "/exams");
        runner.setProperty("f.1", "first:/first");
        runner.setProperty("f.2", "last:/last");
        runner.setProperty("f.3", "grade:/grade");
        runner.setProperty("f.4", "subject:/exams/subject");
        runner.setProperty("f.5", "test:/exams/test");
        runner.setProperty("f.6", "marks:/exams/marks");

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);

            runner.run(1, false);
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testUpdateWithCsv() throws IOException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        final TestableProcessor proc = new TestableProcessor(solrClient);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update/csv");
        runner.setProperty("fieldnames", "first,last,grade,subject,test,marks");

        try (FileInputStream fileIn = new FileInputStream(CSV_MULTIPLE_DOCS_FILE)) {
            runner.enqueue(fileIn);

            runner.run(1, false);
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testUpdateWithXml() throws IOException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        final TestableProcessor proc = new TestableProcessor(solrClient);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update");
        runner.setProperty(PutSolrContentStream.CONTENT_TYPE, "application/xml");

        try (FileInputStream fileIn = new FileInputStream(XML_MULTIPLE_DOCS_FILE)) {
            runner.enqueue(fileIn);

            runner.run(1, false);
            runner.assertTransferCount(PutSolrContentStream.REL_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_CONNECTION_FAILURE, 0);
            runner.assertTransferCount(PutSolrContentStream.REL_SUCCESS, 1);

            verifySolrDocuments(proc.getSolrClient(), Arrays.asList(expectedDoc1, expectedDoc2));
        } finally {
            try {
                proc.getSolrClient().close();
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testDeleteWithXml() throws IOException, SolrServerException {
        final SolrClient solrClient = createEmbeddedSolrClient(DEFAULT_SOLR_CORE);
        final TestableProcessor proc = new TestableProcessor(solrClient);

        final TestRunner runner = createDefaultTestRunner(proc);
        runner.setProperty(PutSolrContentStream.CONTENT_STREAM_PATH, "/update");
        runner.setProperty(PutSolrContentStream.CONTENT_TYPE, "application/xml");
        runner.setProperty("commit", "true");

        // add a document so there is something to delete
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("first", "bob");
        doc.addField("last", "smith");
        doc.addField("created", new Date());

        solrClient.add(doc);
        solrClient.commit();

        // prove the document got added
        SolrQuery query = new SolrQuery("*:*");
        QueryResponse qResponse = solrClient.query(query);
        Assert.assertEquals(1, qResponse.getResults().getNumFound());

        // run the processor with a delete-by-query command
        runner.enqueue("<delete><query>first:bob</query></delete>".getBytes("UTF-8"));
        runner.run(1, false);

        // prove the document got deleted
        qResponse = solrClient.query(query);
        Assert.assertEquals(0, qResponse.getResults().getNumFound());
    }

    @Test
    public void testCollectionExpressionLanguage() throws IOException, SolrServerException {
        final String collection = "collection1";
        final CollectionVerifyingProcessor proc = new CollectionVerifyingProcessor(collection);

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "localhost:9983");
        runner.setProperty(PutSolrContentStream.COLLECTION, "${solr.collection}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("solr.collection", collection);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn, attributes);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_SUCCESS, 1);
        }
    }

    @Test
    public void testSolrServerExceptionShouldRouteToFailure() throws IOException, SolrServerException {
        final Throwable throwable = new SolrServerException("Invalid Document");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }
    }

    @Test
    public void testSolrServerExceptionCausedByIOExceptionShouldRouteToConnectionFailure() throws IOException, SolrServerException {
        final Throwable throwable = new SolrServerException(new IOException("Error communicating with Solr"));
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_CONNECTION_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }
    }

    @Test
    public void testSolrExceptionShouldRouteToFailure() throws IOException, SolrServerException {
        final Throwable throwable = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }
    }

    @Test
    public void testRemoteSolrExceptionShouldRouteToFailure() throws IOException, SolrServerException {
        final Throwable throwable = new HttpSolrClient.RemoteSolrException(
                "host", 401, "error", new NumberFormatException());
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }
    }

    @Test
    public void testIOExceptionShouldRouteToConnectionFailure() throws IOException, SolrServerException {
        final Throwable throwable = new IOException("Error communicating with Solr");
        final ExceptionThrowingProcessor proc = new ExceptionThrowingProcessor(throwable);

        final TestRunner runner = createDefaultTestRunner(proc);

        try (FileInputStream fileIn = new FileInputStream(CUSTOM_JSON_SINGLE_DOC_FILE)) {
            runner.enqueue(fileIn);
            runner.run();

            runner.assertAllFlowFilesTransferred(PutSolrContentStream.REL_CONNECTION_FAILURE, 1);
            verify(proc.getSolrClient(), times(1)).request(any(SolrRequest.class), eq((String)null));
        }
    }

    @Test
    public void testSolrTypeCloudShouldRequireCollection() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertNotValid();

        runner.setProperty(PutSolrContentStream.COLLECTION, "someCollection1");
        runner.assertValid();
    }


    @Test
    public void testSolrTypeStandardShouldNotRequireCollection() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();
    }

    @Test
    public void testHttpsUrlShouldRequireSSLContext() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "https://localhost:8443/solr");
        runner.assertNotValid();

        final SSLContextService sslContextService = new MockSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(PutSolrContentStream.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.assertValid();
    }

    @Test
    public void testHttpUrlShouldNotAllowSSLContext() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();

        final SSLContextService sslContextService = new MockSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(PutSolrContentStream.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.assertNotValid();
    }

    @Test
    public void testJAASClientAppNameValidation() {
        final TestRunner runner = TestRunners.newTestRunner(PutSolrContentStream.class);
        runner.setProperty(PutSolrContentStream.SOLR_TYPE, PutSolrContentStream.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(PutSolrContentStream.SOLR_LOCATION, "http://localhost:8443/solr");
        runner.assertValid();

        // clear the jaas config system property if it was set
        final String jaasConfig = System.getProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
        if (!StringUtils.isEmpty(jaasConfig)) {
            System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
        }

        // should be invalid if we have a client name but not config file
        runner.setProperty(PutSolrContentStream.JAAS_CLIENT_APP_NAME, "Client");
        runner.assertNotValid();

        // should be invalid if we have a client name that is not in the config file
        final File jaasConfigFile = new File("src/test/resources/jaas-client.conf");
        System.setProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP, jaasConfigFile.getAbsolutePath());
        runner.assertNotValid();

        // should be valid now that the name matches up with the config file
        runner.setProperty(PutSolrContentStream.JAAS_CLIENT_APP_NAME, "SolrJClient");
        runner.assertValid();
    }

    /**
     * Mock implementation so we don't need to have a real keystore/truststore available for testing.
     */
    private class MockSSLContextService extends AbstractControllerService implements SSLContextService {

        @Override
        public SSLContext createSSLContext(ClientAuth clientAuth) throws ProcessException {
            return null;
        }

        @Override
        public String getTrustStoreFile() {
            return null;
        }

        @Override
        public String getTrustStoreType() {
            return null;
        }

        @Override
        public String getTrustStorePassword() {
            return null;
        }

        @Override
        public boolean isTrustStoreConfigured() {
            return false;
        }

        @Override
        public String getKeyStoreFile() {
            return null;
        }

        @Override
        public String getKeyStoreType() {
            return null;
        }

        @Override
        public String getKeyStorePassword() {
            return null;
        }

        @Override
        public String getKeyPassword() {
            return null;
        }

        @Override
        public boolean isKeyStoreConfigured() {
            return false;
        }

        @Override
        public String getSslAlgorithm() {
            return null;
        }
    }

    // Override the createSolrClient method to inject a custom SolrClient.
    private class CollectionVerifyingProcessor extends PutSolrContentStream {

        private SolrClient mockSolrClient;

        private final String expectedCollection;

        public CollectionVerifyingProcessor(final String expectedCollection) {
            this.expectedCollection = expectedCollection;
        }

        @Override
        protected SolrClient createSolrClient(ProcessContext context) {
            mockSolrClient = new SolrClient() {
                @Override
                public NamedList<Object> request(SolrRequest solrRequest, String s) throws SolrServerException, IOException {
                    Assert.assertEquals(expectedCollection, solrRequest.getParams().get(PutSolrContentStream.COLLECTION_PARAM_NAME));
                    return new NamedList<>();
                }

                @Override
                public void close() {

                }

            };
            return mockSolrClient;
        }

    }

    // Override the createSolrClient method to inject a Mock.
    private class ExceptionThrowingProcessor extends PutSolrContentStream {

        private SolrClient mockSolrClient;
        private Throwable throwable;

        public ExceptionThrowingProcessor(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        protected SolrClient createSolrClient(ProcessContext context) {
            mockSolrClient = Mockito.mock(SolrClient.class);
            try {
                when(mockSolrClient.request(any(SolrRequest.class),
                        eq((String)null))).thenThrow(throwable);
            } catch (SolrServerException e) {
                Assert.fail(e.getMessage());
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
            return mockSolrClient;
        }

    }

    // Override createSolrClient and return the passed in SolrClient
    private class TestableProcessor extends PutSolrContentStream {
        private SolrClient solrClient;

        public TestableProcessor(SolrClient solrClient) {
            this.solrClient = solrClient;
        }
        @Override
        protected SolrClient createSolrClient(ProcessContext context) {
            return solrClient;
        }
    }

    // Create an EmbeddedSolrClient with the given core name.
    private static SolrClient createEmbeddedSolrClient(String coreName) throws IOException {
        String relPath = TestPutSolrContentStream.class.getProtectionDomain()
                .getCodeSource().getLocation().getFile()
                + "../../target";

        return EmbeddedSolrServerFactory.create(
                EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                coreName, relPath);
    }

    /**
     * Verify that given SolrServer contains the expected SolrDocuments.
     */
    private static void verifySolrDocuments(SolrClient solrServer, Collection<SolrDocument> expectedDocuments)
            throws IOException, SolrServerException {

        solrServer.commit();

        SolrQuery query = new SolrQuery("*:*");
        QueryResponse qResponse = solrServer.query(query);
        Assert.assertEquals(expectedDocuments.size(), qResponse.getResults().getNumFound());

        // verify documents have expected fields and values
        for (SolrDocument expectedDoc : expectedDocuments) {
            boolean found = false;
            for (SolrDocument solrDocument : qResponse.getResults()) {
                boolean foundAllFields = true;
                for (String expectedField : expectedDoc.getFieldNames()) {
                    Object expectedVal = expectedDoc.getFirstValue(expectedField);
                    Object actualVal = solrDocument.getFirstValue(expectedField);
                    foundAllFields = expectedVal.equals(actualVal);
                }

                if (foundAllFields) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("Could not find " + expectedDoc, found);
        }
    }

}
