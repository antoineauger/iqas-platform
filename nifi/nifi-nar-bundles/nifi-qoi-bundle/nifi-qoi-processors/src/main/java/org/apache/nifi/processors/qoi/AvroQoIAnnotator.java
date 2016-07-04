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
package org.apache.nifi.processors.qoi;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@EventDriven
@Tags({"qoi", "annotation"})
@CapabilityDescription("Allows the user to select QoI attributes of interest to annotate an Avro FlowFile")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "Own-defined QoI attributes", value = "Attribute Expression Language", supportsExpressionLanguage = true, description = "QoI attributes of interest for the user")
/*@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})*/
public class AvroQoIAnnotator extends AbstractProcessor {

    /**
     * Custom QoI validator
     */
    public static final Validator QOI_ATTRIBUTES_DEFINITION_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject("Unknown QoI attribute").input(subject);

            Map<String, PropertyValue> qoiAttrAlreadyDefined = new HashMap<>();
            String[] qoiAttrList = input.split(",");

            for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (descriptor.isDynamic()) {
                    qoiAttrAlreadyDefined.put(descriptor.getName(), context.getProperty(descriptor));
                }
            }

            for (String s : qoiAttrList) {
                if (!qoiAttrAlreadyDefined.containsKey(s)) {
                    return builder.valid(false).explanation("there is at least one unknown QoI attribute (" + s + "). Please define it prior to continue.").build();
                }
            }

            return builder.valid(true).explanation("All QoI attributes have been well-defined").build();
        }
    };

    /**
     * Properties
     */
    private static final PropertyDescriptor QOI_ATTR = new PropertyDescriptor.Builder()
            .name("QoI attributes")
            .description("Comma-separated QoI attributes to add to the Avro FlowFile.")
            .addValidator(QOI_ATTRIBUTES_DEFINITION_VALIDATOR)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    private static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Avro schema")
            .description("If the Avro records do not contain the schema (datum only), it must be specified here.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .build();

    /**
     * Relationships
     * REL_SUCCESS, REL_COMPUT_FAILURE and REL_FAILURE
     */
    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The specified QoI attributes of interest are correctly added to an Avro FlowFile")
            .build();
    private static final Relationship REL_COMPUT_FAILURE = new Relationship.Builder()
            .name("QoI attribute computation failure")
            .description("A FlowFile is routed to this relationship if the computation of one or several QoI attributes fails")
            .build();
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro")
            .build();

    private List<PropertyDescriptor> properties;
    private volatile Schema schema = null;
    private Set<Relationship> relationships;
    private volatile Map<String, PropertyValue> qoiAttrToAnnotate = new HashMap<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QOI_ATTR);
        properties.add(SCHEMA);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_COMPUT_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        getLogger().error("A dynamic property has been modified: {} {} {}", new Object[]{descriptor.getName(), oldValue, newValue});
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Map<String, PropertyValue> newQoiAttrToAnnotate = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            getLogger().error("Adding new dynamic property: {} {}", new Object[]{descriptor.getName(), context.getProperty(descriptor)});
            newQoiAttrToAnnotate.put(descriptor.getName(), context.getProperty(descriptor));
        }

        this.qoiAttrToAnnotate = newQoiAttrToAnnotate;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        if ( original == null ) {
            return;
        }

        Map<String,String> qoiAttributes = new HashMap<>();
        final String stringSchema = context.getProperty(SCHEMA).getValue();
        final boolean schemaLess = stringSchema != null;

        try {
            original = session.write(original, new StreamCallback() {
                @Override
                public void process(InputStream rawIn, OutputStream rawOut) throws IOException {
                    GenericRecord record = null;

                    if (schemaLess) {
                        if (schema == null) {
                            schema = new Schema.Parser().parse(stringSchema);
                        }
                        try (final InputStream in = new BufferedInputStream(rawIn);
                             final OutputStream out = new BufferedOutputStream(rawOut)) {

                            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
                            record = reader.read(null, decoder);

                            //TODO process record
                            //out.write(stringSchema.getBytes(StandardCharsets.UTF_8));
                        }
                    }
                    else {

                        try (final InputStream in = new BufferedInputStream(rawIn);
                             final OutputStream out = new BufferedOutputStream(rawOut)) {

                            try (final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {
                                if (reader.hasNext()) {
                                    record = reader.next();
                                }

                                //TODO process record

                                String timeliness = String.valueOf(record.get("time"));
                                getLogger().error("QOI ATTR1 " + record.toString());
                                getLogger().error("QOI ATTR2 " + record.get("time").toString());
                                getLogger().error("QOI ATTR4 " + String.valueOf(timeliness));
                                qoiAttributes.put("timeliness", String.valueOf(record.get("time")));

                                out.write("PARSED".getBytes(StandardCharsets.UTF_8));
                                out.write("timeliness".getBytes(StandardCharsets.UTF_8));
                                out.write(String.valueOf(record.get("time")).getBytes(StandardCharsets.UTF_8));
                            }
                        }

                    }

                }

            });

        } catch (final ProcessException pe) {
            getLogger().error("Failed to convert {} from Avro to JSON due to {}; transferring to failure", new Object[]{original, pe});
            session.transfer(original, REL_FAILURE);
            return;
        }

        original = session.putAllAttributes(original, qoiAttributes);
        session.transfer(original, REL_SUCCESS);
    }
}
