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

package org.apache.nifi.controller;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.dom.DOMSource;

import org.apache.nifi.persistence.TemplateDeserializer;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.w3c.dom.Element;

public class TemplateUtils {

    public static TemplateDTO parseDto(final Element templateElement) {
        try {
            JAXBContext context = JAXBContext.newInstance(TemplateDTO.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            return unmarshaller.unmarshal(new DOMSource(templateElement), TemplateDTO.class).getValue();
        } catch (final Exception e) {
            throw new RuntimeException("Could not parse XML as a valid template", e);
        }
    }

    public static TemplateDTO parseDto(final byte[] bytes) {
        try (final InputStream in = new ByteArrayInputStream(bytes)) {
            return TemplateDeserializer.deserialize(in);
        } catch (final IOException ioe) {
            throw new RuntimeException("Could not parse bytes as template", ioe); // won't happen because of the types of streams being used
        }
    }

    public static List<Template> parseTemplateStream(final byte[] bytes) {
        final List<Template> templates = new ArrayList<>();

        try (final InputStream rawIn = new ByteArrayInputStream(bytes);
            final DataInputStream in = new DataInputStream(rawIn)) {

            while (isMoreData(in)) {
                final int length = in.readInt();
                final byte[] buffer = new byte[length];
                StreamUtils.fillBuffer(in, buffer, true);
                final TemplateDTO dto = TemplateDeserializer.deserialize(new ByteArrayInputStream(buffer));
                templates.add(new Template(dto));
            }
        } catch (final IOException e) {
            throw new RuntimeException("Could not parse bytes", e);  // won't happen because of the types of streams being used
        }

        return templates;
    }


    private static boolean isMoreData(final InputStream in) throws IOException {
        in.mark(1);
        final int nextByte = in.read();
        if (nextByte == -1) {
            return false;
        }

        in.reset();
        return true;
    }

    /**
     * Scrubs the template prior to persisting in order to remove fields that shouldn't be included or are unnecessary.
     *
     * @param templateDto template
     */
    public static void scrubTemplate(final TemplateDTO templateDto) {
        scrubSnippet(templateDto.getSnippet());
    }

    private static void scrubSnippet(final FlowSnippetDTO snippet) {
        // ensure that contents have been specified
        if (snippet != null) {
            // go through each processor if specified
            if (snippet.getProcessors() != null) {
                scrubProcessors(snippet.getProcessors());
            }

            // go through each connection if specified
            if (snippet.getConnections() != null) {
                scrubConnections(snippet.getConnections());
            }

            // go through each remote process group if specified
            if (snippet.getRemoteProcessGroups() != null) {
                scrubRemoteProcessGroups(snippet.getRemoteProcessGroups());
            }

            // go through each process group if specified
            if (snippet.getProcessGroups() != null) {
                scrubProcessGroups(snippet.getProcessGroups());
            }

            // go through each controller service if specified
            if (snippet.getControllerServices() != null) {
                scrubControllerServices(snippet.getControllerServices());
            }
        }
    }

    /**
     * Scrubs process groups prior to saving.
     *
     * @param processGroups groups
     */
    private static void scrubProcessGroups(final Set<ProcessGroupDTO> processGroups) {
        // go through each process group
        for (final ProcessGroupDTO processGroupDTO : processGroups) {
            scrubSnippet(processGroupDTO.getContents());
        }
    }

    /**
     * Scrubs processors prior to saving. This includes removing sensitive properties, validation errors, property descriptors, etc.
     *
     * @param processors procs
     */
    private static void scrubProcessors(final Set<ProcessorDTO> processors) {
        // go through each processor
        for (final ProcessorDTO processorDTO : processors) {
            final ProcessorConfigDTO processorConfig = processorDTO.getConfig();

            // ensure that some property configuration have been specified
            if (processorConfig != null) {
                // if properties have been specified, remove sensitive ones
                if (processorConfig.getProperties() != null) {
                    Map<String, String> processorProperties = processorConfig.getProperties();

                    // look for sensitive properties and remove them
                    if (processorConfig.getDescriptors() != null) {
                        final Collection<PropertyDescriptorDTO> descriptors = processorConfig.getDescriptors().values();
                        for (PropertyDescriptorDTO descriptor : descriptors) {
                            if (descriptor.isSensitive()) {
                                processorProperties.put(descriptor.getName(), null);
                            }
                        }
                    }
                }

                processorConfig.setCustomUiUrl(null);
            }

            // remove validation errors
            processorDTO.setValidationErrors(null);
            processorDTO.setInputRequirement(null);
        }
    }

    private static void scrubControllerServices(final Set<ControllerServiceDTO> controllerServices) {
        for (final ControllerServiceDTO serviceDTO : controllerServices) {
            final Map<String, String> properties = serviceDTO.getProperties();
            final Map<String, PropertyDescriptorDTO> descriptors = serviceDTO.getDescriptors();

            if (properties != null && descriptors != null) {
                for (final PropertyDescriptorDTO descriptor : descriptors.values()) {
                    if (descriptor.isSensitive()) {
                        properties.put(descriptor.getName(), null);
                    }
                }
            }

            serviceDTO.setCustomUiUrl(null);
            serviceDTO.setValidationErrors(null);
        }
    }

    /**
     * Scrubs connections prior to saving. This includes removing available relationships.
     *
     * @param connections conns
     */
    private static void scrubConnections(final Set<ConnectionDTO> connections) {
        // go through each connection
        for (final ConnectionDTO connectionDTO : connections) {
            connectionDTO.setAvailableRelationships(null);

            scrubConnectable(connectionDTO.getSource());
            scrubConnectable(connectionDTO.getDestination());
        }
    }

    /**
     * Remove unnecessary fields in connectables prior to saving.
     *
     * @param connectable connectable
     */
    private static void scrubConnectable(final ConnectableDTO connectable) {
        if (connectable != null) {
            connectable.setComments(null);
            connectable.setExists(null);
            connectable.setRunning(null);
            connectable.setTransmitting(null);
            connectable.setName(null);
        }
    }

    /**
     * Remove unnecessary fields in remote groups prior to saving.
     *
     * @param remoteGroups groups
     */
    private static void scrubRemoteProcessGroups(final Set<RemoteProcessGroupDTO> remoteGroups) {
        // go through each remote process group
        for (final RemoteProcessGroupDTO remoteProcessGroupDTO : remoteGroups) {
            remoteProcessGroupDTO.setFlowRefreshed(null);
            remoteProcessGroupDTO.setInputPortCount(null);
            remoteProcessGroupDTO.setOutputPortCount(null);
            remoteProcessGroupDTO.setTransmitting(null);
            remoteProcessGroupDTO.setProxyPassword(null);

            // if this remote process group has contents
            if (remoteProcessGroupDTO.getContents() != null) {
                RemoteProcessGroupContentsDTO contents = remoteProcessGroupDTO.getContents();

                // scrub any remote input ports
                if (contents.getInputPorts() != null) {
                    scrubRemotePorts(contents.getInputPorts());
                }

                // scrub and remote output ports
                if (contents.getOutputPorts() != null) {
                    scrubRemotePorts(contents.getOutputPorts());
                }
            }
        }
    }

    /**
     * Remove unnecessary fields in remote ports prior to saving.
     *
     * @param remotePorts ports
     */
    private static void scrubRemotePorts(final Set<RemoteProcessGroupPortDTO> remotePorts) {
        for (final Iterator<RemoteProcessGroupPortDTO> remotePortIter = remotePorts.iterator(); remotePortIter.hasNext();) {
            final RemoteProcessGroupPortDTO remotePortDTO = remotePortIter.next();

            // if the flow is not connected to this remote port, remove it
            if (remotePortDTO.isConnected() == null || !remotePortDTO.isConnected().booleanValue()) {
                remotePortIter.remove();
                continue;
            }

            remotePortDTO.setExists(null);
            remotePortDTO.setTargetRunning(null);
        }
    }
}
