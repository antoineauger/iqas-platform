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
package org.apache.nifi.processors.sensors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"iqas", "sensor", "virtual"})
@CapabilityDescription("Allow the user to set up a virtual sensor for iQAS testing purpose. " +
        "Virtual sensors generate data from a disk file (to adjust in processor's settings) and the output format of observations is JSON.")
public class VirtualSensor extends AbstractProcessor {

    /**
     * Properties
     */
    private static final PropertyDescriptor OBS_FILE_PATH = new PropertyDescriptor.Builder()
            .name("File path for observations to send")
            .description("Absolute path for the file containing all observations to produce. " +
                    "Each observation record must be in JSON format, and it must only be one observation per line.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("${absolute.path}/${filename}")
            .dynamic(false)
            .required(true)
            .required(true)
            .build();

    /**
     * Relationships
     * REL_SUCCESS, REL_COMPUT_FAILURE and REL_FAILURE
     */
    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A flowfile is routed to this relationship if the observation record has been correctly processed and converted into JSON format")
            .build();
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A flowfile is routed to this relationship if the specified file contains errors or if source data are not JSON compliant")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(OBS_FILE_PATH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        // TODO implement
    }
}
