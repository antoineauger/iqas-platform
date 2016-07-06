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
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.annotation.behavior.*;
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
import java.util.*;

@EventDriven
@Tags({"qoi", "annotation", "avro"})
@CapabilityDescription("Allows the user to select QoI attributes of interest to annotate an Avro flowfile. " +
        "QoI attributes are computed according to their definition as dynamic properties. " +
        "Then, QoI attributes are added to each Avro record under the property 'qoi'. " +
        "The outgoing Avro schema is modified consequently to take into account that change.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "Own-defined QoI attributes", value = "Attribute Expression Language", supportsExpressionLanguage = true, description = "QoI attributes of interest for the user")
@WritesAttributes({
        @WritesAttribute(attribute = "avail_qoi_attr_metadata", description = "TODO"),
        @WritesAttribute(attribute = "qoi", description = "TODO"),
        @WritesAttribute(attribute = "avail_qoi_attr_content", description = "TODO")
})
/*@ReadsAttributes({@ReadsAttribute(attribute="", description="")})*/
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
                    return builder.valid(false).explanation("there is at least one unknown QoI attribute (" + s + "). " +
                            "Please define it as a dynamic property prior to continue.").build();
                }
            }

            return builder.valid(true).explanation("All QoI attributes have been well-defined").build();
        }
    };

    /**
     * Properties
     */

    public static final String DESTINATION_ATTRIBUTE = "attribute";
    public static final String DESTINATION_CONTENT = "content";
    public static final String DESTINATION_BOTH = "content and attribute";

    private static final PropertyDescriptor QOI_CHECKPOINT_NAME = new PropertyDescriptor.Builder()
            .name("QoI checkpoint name")
            .description("The string key to group all QoI attributes for this processor.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .dynamic(false)
            .required(true)
            .build();

    private static final PropertyDescriptor QOI_ATTR_TO_ANNOTATE = new PropertyDescriptor.Builder()
            .name("QoI attributes")
            .description("Comma-separated QoI attributes to add to the Avro flowfile.")
            .addValidator(QOI_ATTRIBUTES_DEFINITION_VALIDATOR)
            .expressionLanguageSupported(false)
            .dynamic(false)
            .required(true)
            .build();

    private static final PropertyDescriptor AVRO_ATTR_TO_IMPORT = new PropertyDescriptor.Builder()
            .name("Avro attributes to import")
            .description("Comma-separated QoI attributes to import from Avro records. " +
                    "Then, the value can be used in a dynamic property with ${avro_attr}.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .dynamic(false)
            .required(false)
            .build();

    private static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Control if QoI attributes are written as a new flowfile attribute " +
                    ", written in the flowfile content or both. Writing to flowfile content will overwrite any " +
                    "existing flowfile content.")
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT, DESTINATION_BOTH)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .expressionLanguageSupported(false)
            .dynamic(false)
            .required(true)
            .build();

    private static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Avro schema")
            .description("If the Avro records do not contain the schema (datum only), it must be specified here.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .dynamic(false)
            .required(false)
            .build();

    /**
     * Relationships
     * REL_SUCCESS, REL_COMPUT_FAILURE and REL_FAILURE
     */
    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The specified QoI attributes of interest are correctly added to an Avro flowfile")
            .build();
    private static final Relationship REL_COMPUT_FAILURE = new Relationship.Builder()
            .name("computation failure")
            .description("A flowfile is routed to this relationship if the computation of one or several QoI attributes fails")
            .build();
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A flowfile is routed to this relationship if it cannot be parsed as Avro")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private volatile Schema schema = null;
    private volatile String checkpointName = "";
    private volatile Map<String, PropertyValue> qoiAttrToAnnotate = new HashMap<>();
    private volatile ArrayList<String> avroAttrListToImport = new ArrayList<>();
    private volatile Map<String,String> knownProperties = new HashMap<>();
    private volatile Map<String,String> qoiAttrToAppendAttr = new HashMap<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QOI_CHECKPOINT_NAME);
        properties.add(QOI_ATTR_TO_ANNOTATE);
        properties.add(AVRO_ATTR_TO_IMPORT);
        properties.add(DESTINATION);
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
        if (descriptor.isDynamic() && newValue == null) {
            this.qoiAttrToAnnotate.remove(descriptor.getName());
            this.qoiAttrToAppendAttr.remove(descriptor.getName());
            this.knownProperties.remove(descriptor.getName());
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.checkpointName = context.getProperty(QOI_CHECKPOINT_NAME).getValue();
        this.qoiAttrToAnnotate.clear();
        this.knownProperties.clear();
        final Map<String, PropertyValue> newQoiAttrToAnnotate = new HashMap<>();

        ArrayList<String> qoiAttrList = new ArrayList<>();
        qoiAttrList.addAll(Arrays.asList(context.getProperty(QOI_ATTR_TO_ANNOTATE).getValue().split(",")));
        avroAttrListToImport.addAll(Arrays.asList(context.getProperty(AVRO_ATTR_TO_IMPORT).getValue().split(",")));

        for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic() && qoiAttrList.contains(descriptor.getName())) {
                getLogger().error("Adding new QoI attribute: {} {}", new Object[]{descriptor.getName(), context.getProperty(descriptor)});
                newQoiAttrToAnnotate.put(descriptor.getName(), context.getProperty(descriptor));
                this.knownProperties.put(descriptor.getName(), context.getProperty(descriptor).getValue());
            }
        }

        this.qoiAttrToAnnotate = newQoiAttrToAnnotate;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        final FlowFile copyForAttributes = session.clone(original);
        if ( original == null ) {
            return;
        }

        final String stringSchema = context.getProperty(SCHEMA).getValue();
        final boolean schemaLess = stringSchema != null;
        Set<String> missingAvroFields = new HashSet<>();

        try {
            original = session.write(original, new StreamCallback() {
                @Override
                public void process(InputStream rawIn, OutputStream rawOut) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn);
                         final OutputStream out = new BufferedOutputStream(rawOut)) {
                        GenericRecord record = null;

                        if (schemaLess) {
                            if (schema == null) {
                                schema = new Schema.Parser().parse(stringSchema);
                            }
                            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
                            record = reader.read(null, decoder);
                        } else {
                            DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
                            if (reader.hasNext()) {
                                record = reader.next();
                            }
                            schema = record.getSchema();
                        }

                        // Avro fields import
                        for (String s : avroAttrListToImport) {
                            if (record.get(s) != null) {
                                knownProperties.put(s, record.get(s).toString());
                            } else {
                                missingAvroFields.add(s);
                            }
                        }

                        // Definition of the new Avro record
                        Schema newSchema = AvroUtil.buildGlobalSchema(record.getSchema().getType().toString().toLowerCase(),
                                record.getSchema().getName(),
                                record.getSchema().getNamespace(),
                                record.getSchema().getFields());

                        // Preserve the given fields from the old Avro record
                        GenericRecord newRecord = AvroUtil.copyFields(record,record.getSchema().getFields(),newSchema);

                        // QoI annotation
                        for (String s : qoiAttrToAnnotate.keySet()) {
                            qoiAttrToAppendAttr.put(s,context.getProperty(s).evaluateAttributeExpressions(copyForAttributes, knownProperties).getValue());
                        }
                        newRecord = AvroUtil.annotateRecordWithQoIAttr(newRecord,checkpointName,qoiAttrToAppendAttr);

                        // Set the writer for Avro records
                        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(newSchema);
                        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);

                        // Destination for QoI attributes
                        switch (context.getProperty(DESTINATION).getValue()) {
                            case DESTINATION_ATTRIBUTE:
                                // Attribute destination already done
                                break;
                            case DESTINATION_CONTENT:
                                // No attribute destination
                                qoiAttrToAppendAttr.clear();
                                // Content destination
                                try (DataFileWriter<GenericRecord> w = writer.create(newRecord.getSchema(), out)) {
                                    w.append(newRecord);
                                    w.close();
                                } catch (Exception e) {
                                    getLogger().error(e.toString());
                                }
                                break;
                            case DESTINATION_BOTH:
                                // Attribute destination already done
                                // Content destination
                                try (DataFileWriter<GenericRecord> w = writer.create(newRecord.getSchema(), out)) {
                                    w.append(newRecord);
                                    w.close();
                                } catch (Exception e) {
                                    getLogger().error(e.toString());
                                }
                                break;
                        }

                    }
                }

            });
        } catch (final ProcessException e) {
            getLogger().error("Failed to convert {} from Avro to JSON due to {}; transferring to failure", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
            session.remove(copyForAttributes);
            return;
        }

        if (missingAvroFields.size() == 0) {
            original = session.putAllAttributes(original, qoiAttrToAppendAttr);
            session.transfer(original, REL_SUCCESS);
            session.remove(copyForAttributes);
        }
        else {
            getLogger().error("Unable to compute some QoI attributes using Avro fields. Missing Avro fields: " + missingAvroFields.toString());
            original = session.putAllAttributes(original, qoiAttrToAppendAttr);
            session.transfer(original, REL_COMPUT_FAILURE);
            session.remove(copyForAttributes);
        }
    }

}
