/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStreamBase;

@Tags({"Apache", "Solr", "Put", "Send"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends the contents of a FlowFile as a ContentStream to Solr")
@DynamicProperty(name="A Solr request parameter name", value="A Solr request parameter value",
        description="These parameters will be passed to Solr on the request")
public class PutSolrContentStream extends SolrProcessor {

    public static final PropertyDescriptor CONTENT_STREAM_PATH = new PropertyDescriptor
            .Builder().name("Content Stream Path")
            .description("The path in Solr to post the ContentStream")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("/update/json/docs")
            .build();

    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor
            .Builder().name("Content-Type")
            .description("Content-Type being sent to Solr")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("application/json")
            .build();

    public static final PropertyDescriptor COMMIT_WITHIN = new PropertyDescriptor
            .Builder().name("Commit Within")
            .description("The number of milliseconds before the given update is committed")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The original FlowFile")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed for any reason other than Solr being unreachable")
            .build();

    public static final Relationship REL_CONNECTION_FAILURE = new Relationship.Builder()
            .name("connection_failure")
            .description("FlowFiles that failed because Solr is unreachable")
            .build();

    public static final String COLLECTION_PARAM_NAME = "collection";
    public static final String COMMIT_WITHIN_PARAM_NAME = "commitWithin";
    public static final String REPEATING_PARAM_PATTERN = "\\w+\\.\\d+";

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(COLLECTION);
        descriptors.add(CONTENT_STREAM_PATH);
        descriptors.add(CONTENT_TYPE);
        descriptors.add(COMMIT_WITHIN);
        descriptors.add(SOLR_SOCKET_TIMEOUT);
        descriptors.add(SOLR_CONNECTION_TIMEOUT);
        descriptors.add(SOLR_MAX_CONNECTIONS);
        descriptors.add(SOLR_MAX_CONNECTIONS_PER_HOST);
        descriptors.add(ZK_CLIENT_TIMEOUT);
        descriptors.add(ZK_CONNECTION_TIMEOUT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_CONNECTION_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value to send for the '" + propertyDescriptorName + "' request parameter")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final AtomicReference<Exception> error = new AtomicReference<>(null);
        final AtomicReference<Exception> connectionError = new AtomicReference<>(null);

        final boolean isSolrCloud = SOLR_TYPE_CLOUD.equals(context.getProperty(SOLR_TYPE).getValue());
        final String collection = context.getProperty(COLLECTION).evaluateAttributeExpressions(flowFile).getValue();
        final Long commitWithin = context.getProperty(COMMIT_WITHIN).evaluateAttributeExpressions(flowFile).asLong();

        final MultiMapSolrParams requestParams = new MultiMapSolrParams(getRequestParams(context, flowFile));

        StopWatch timer = new StopWatch(true);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                final String contentStreamPath = context.getProperty(CONTENT_STREAM_PATH)
                        .evaluateAttributeExpressions().getValue();

                ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(contentStreamPath);
                request.setParams(new ModifiableSolrParams());

                // add the extra params, don't use 'set' in case of repeating params
                Iterator<String> paramNames = requestParams.getParameterNamesIterator();
                while (paramNames.hasNext()) {
                    String paramName = paramNames.next();
                    for (String paramValue : requestParams.getParams(paramName)) {
                        request.getParams().add(paramName, paramValue);
                    }
                }

                // specify the collection for SolrCloud
                if (isSolrCloud) {
                    request.setParam(COLLECTION_PARAM_NAME, collection);
                }

                if (commitWithin != null && commitWithin > 0) {
                    request.setParam(COMMIT_WITHIN_PARAM_NAME, commitWithin.toString());
                }

                try (final BufferedInputStream bufferedIn = new BufferedInputStream(in)) {
                    // add the FlowFile's content on the UpdateRequest
                    request.addContentStream(new ContentStreamBase() {
                        @Override
                        public InputStream getStream() throws IOException {
                            return bufferedIn;
                        }

                        @Override
                        public String getContentType() {
                            return context.getProperty(CONTENT_TYPE).evaluateAttributeExpressions().getValue();
                        }
                    });

                    UpdateResponse response = request.process(getSolrClient());
                    getLogger().debug("Got {} response from Solr", new Object[]{response.getStatus()});
                } catch (SolrException e) {
                    error.set(e);
                } catch (SolrServerException e) {
                    if (causedByIOException(e)) {
                        connectionError.set(e);
                    } else {
                        error.set(e);
                    }
                } catch (IOException e) {
                    connectionError.set(e);
                }
            }
        });
        timer.stop();

        if (error.get() != null) {
            getLogger().error("Failed to send {} to Solr due to {}; routing to failure",
                    new Object[]{flowFile, error.get()});
            session.transfer(flowFile, REL_FAILURE);
        } else if (connectionError.get() != null) {
            getLogger().error("Failed to send {} to Solr due to {}; routing to connection_failure",
                    new Object[]{flowFile, connectionError.get()});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_CONNECTION_FAILURE);
        } else {
            StringBuilder transitUri = new StringBuilder("solr://");
            transitUri.append(context.getProperty(SOLR_LOCATION).getValue());
            if (isSolrCloud) {
                transitUri.append(":").append(collection);
            }

            final long duration = timer.getDuration(TimeUnit.MILLISECONDS);
            session.getProvenanceReporter().send(flowFile, transitUri.toString(), duration, true);
            getLogger().info("Successfully sent {} to Solr in {} millis", new Object[]{flowFile, duration});
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private boolean causedByIOException(SolrServerException e) {
        boolean foundIOException = false;
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof IOException) {
                foundIOException = true;
                break;
            }
            cause = cause.getCause();
        }
        return foundIOException;
    }

    // get all of the dynamic properties and values into a Map for later adding to the Solr request
    private Map<String, String[]> getRequestParams(ProcessContext context, FlowFile flowFile) {
        final Map<String,String[]> paramsMap = new HashMap<>();
        final SortedMap<String,String> repeatingParams = new TreeMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                final String paramName = descriptor.getName();
                final String paramValue = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();

                if (!paramValue.trim().isEmpty()) {
                    if (paramName.matches(REPEATING_PARAM_PATTERN)) {
                        repeatingParams.put(paramName, paramValue);
                    } else {
                        MultiMapSolrParams.addParam(paramName, paramValue, paramsMap);
                    }
                }
            }
        }

        for (final Map.Entry<String,String> entry : repeatingParams.entrySet()) {
            final String paramName = entry.getKey();
            final String paramValue = entry.getValue();
            final int idx = paramName.lastIndexOf(".");
            MultiMapSolrParams.addParam(paramName.substring(0, idx), paramValue, paramsMap);
        }

        return paramsMap;
    }

}
