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
package org.apache.nifi.documentation;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.documentation.html.HtmlDocumentationWriter;
import org.apache.nifi.documentation.html.HtmlProcessorDocumentationWriter;
import org.apache.nifi.documentation.init.ControllerServiceInitializer;
import org.apache.nifi.documentation.init.ProcessorInitializer;
import org.apache.nifi.documentation.init.ReportingTaskingInitializer;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the ExtensionManager to get a list of Processor, ControllerService, and
 * Reporting Task classes that were loaded and generate documentation for them.
 *
 *
 */
public class DocGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DocGenerator.class);

    /**
     * Generates documentation into the work/docs dir specified by
     * NiFiProperties.
     *
     * @param properties to lookup nifi properties
     */
    public static void generate(final NiFiProperties properties) {
        @SuppressWarnings("rawtypes")
        final Set<Class> extensionClasses = new HashSet<>();
        extensionClasses.addAll(ExtensionManager.getExtensions(Processor.class));
        extensionClasses.addAll(ExtensionManager.getExtensions(ControllerService.class));
        extensionClasses.addAll(ExtensionManager.getExtensions(ReportingTask.class));

        final File explodedNiFiDocsDir = properties.getComponentDocumentationWorkingDirectory();

        logger.debug("Generating documentation for: " + extensionClasses.size() + " components in: "
                + explodedNiFiDocsDir);

        for (final Class<?> extensionClass : extensionClasses) {
            if (ConfigurableComponent.class.isAssignableFrom(extensionClass)) {
                final Class<? extends ConfigurableComponent> componentClass = extensionClass.asSubclass(ConfigurableComponent.class);
                try {
                    logger.debug("Documenting: " + componentClass);
                    document(explodedNiFiDocsDir, componentClass);
                } catch (Exception e) {
                    logger.warn("Unable to document: " + componentClass, e);
                }
            }
        }
    }

    /**
     * Generates the documentation for a particular configurable component. Will
     * check to see if an "additionalDetails.html" file exists and will link
     * that from the generated documentation.
     *
     * @param docsDir the work\docs\components dir to stick component
     * documentation in
     * @param componentClass the class to document
     * @throws InstantiationException ie
     * @throws IllegalAccessException iae
     * @throws IOException ioe
     * @throws InitializationException ie
     */
    private static void document(final File docsDir, final Class<? extends ConfigurableComponent> componentClass)
            throws InstantiationException, IllegalAccessException, IOException, InitializationException {

        final ConfigurableComponent component = componentClass.newInstance();
        final ConfigurableComponentInitializer initializer = getComponentInitializer(componentClass);
        initializer.initialize(component);

        final DocumentationWriter writer = getDocumentWriter(componentClass);

        final File directory = new File(docsDir, componentClass.getCanonicalName());
        directory.mkdirs();

        final File baseDocumentationFile = new File(directory, "index.html");
        if (baseDocumentationFile.exists()) {
            logger.warn(baseDocumentationFile + " already exists, overwriting!");
        }

        try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(baseDocumentationFile))) {
            writer.write(component, output, hasAdditionalInfo(directory));
        }

        initializer.teardown(component);
    }

    /**
     * Returns the DocumentationWriter for the type of component. Currently
     * Processor, ControllerService, and ReportingTask are supported.
     *
     * @param componentClass the class that requires a DocumentationWriter
     * @return a DocumentationWriter capable of generating documentation for
     * that specific type of class
     */
    private static DocumentationWriter getDocumentWriter(final Class<? extends ConfigurableComponent> componentClass) {
        if (Processor.class.isAssignableFrom(componentClass)) {
            return new HtmlProcessorDocumentationWriter();
        } else if (ControllerService.class.isAssignableFrom(componentClass)) {
            return new HtmlDocumentationWriter();
        } else if (ReportingTask.class.isAssignableFrom(componentClass)) {
            return new HtmlDocumentationWriter();
        }

        return null;
    }

    /**
     * Returns a ConfigurableComponentInitializer for the type of component.
     * Currently Processor, ControllerService and ReportingTask are supported.
     *
     * @param componentClass the class that requires a
     * ConfigurableComponentInitializer
     * @return a ConfigurableComponentInitializer capable of initializing that
     * specific type of class
     */
    private static ConfigurableComponentInitializer getComponentInitializer(
            final Class<? extends ConfigurableComponent> componentClass) {
        if (Processor.class.isAssignableFrom(componentClass)) {
            return new ProcessorInitializer();
        } else if (ControllerService.class.isAssignableFrom(componentClass)) {
            return new ControllerServiceInitializer();
        } else if (ReportingTask.class.isAssignableFrom(componentClass)) {
            return new ReportingTaskingInitializer();
        }

        return null;
    }

    /**
     * Checks to see if a directory to write to has an additionalDetails.html in
     * it already.
     *
     * @param directory to check
     * @return true if additionalDetails.html exists, false otherwise.
     */
    private static boolean hasAdditionalInfo(File directory) {
        return directory.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.equalsIgnoreCase(HtmlDocumentationWriter.ADDITIONAL_DETAILS_HTML);
            }

        }).length > 0;
    }
}
