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
@CapabilityDescription("Allows the user to select QoI attributes of interest to annotate an Avro FlowFile. " +
        "QoI attributes are computed according to their definition as dynamic properties. " +
        "Then, QoI attributes are added to each Avro record under the property 'qoi'. " +
        "The outgoing Avro schema is modified consequently to take into account that change.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "Own-defined QoI attributes", value = "Attribute Expression Language", supportsExpressionLanguage = true, description = "QoI attributes of interest for the user")
@WritesAttributes({
        @WritesAttribute(attribute = "avail_qoi_attr_metadata", description = "TODO"),
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
            .description("Comma-separated QoI attributes to add to the Avro FlowFile.")
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
            .description("The specified QoI attributes of interest are correctly added to an Avro FlowFile")
            .build();
    private static final Relationship REL_COMPUT_FAILURE = new Relationship.Builder()
            .name("computation failure")
            .description("A FlowFile is routed to this relationship if the computation of one or several QoI attributes fails")
            .build();
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private volatile Schema schema = null;
    private volatile Map<String, PropertyValue> qoiAttrToAnnotate = new HashMap<>();
    private volatile ArrayList<String> avroAttrListToImport = new ArrayList<>();
    private volatile Map<String,String> knownProperties = new HashMap<>();
    private volatile Map<String,String> qoiAttrToWriteToFlow = new HashMap<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QOI_CHECKPOINT_NAME);
        properties.add(QOI_ATTR_TO_ANNOTATE);
        properties.add(AVRO_ATTR_TO_IMPORT);
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
            this.qoiAttrToWriteToFlow.remove(descriptor.getName());
            this.knownProperties.remove(descriptor.getName());
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
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

        try {
            FlowFile finalOriginal = original;
            original = session.write(original, new StreamCallback() {
                @Override
                public void process(InputStream rawIn, OutputStream rawOut) throws IOException {
                    String tempValue = "";
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
                                for (String s : avroAttrListToImport) {
                                    if (record.get(s) != null) {
                                        knownProperties.put(s, record.get(s).toString());
                                    }
                                    else {
                                        throw new NoSuchAvroFieldException(s);
                                    }

                                }

                                for (String s : qoiAttrToAnnotate.keySet()) {
                                    tempValue = context.getProperty(s).evaluateAttributeExpressions(copyForAttributes, knownProperties).getValue();
                                    qoiAttrToWriteToFlow.put(s, tempValue);
                                }

                                //TODO write in header / avro schema / both
                            } catch (NoSuchAvroFieldException e) {
                                throw new ProcessException(e);
                            }

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

        original = session.putAllAttributes(original, qoiAttrToWriteToFlow);
        session.transfer(original, REL_SUCCESS);
        session.remove(copyForAttributes);
    }

}
