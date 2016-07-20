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
package org.apache.nifi.authorization;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.file.generated.Authorizations;
import org.apache.nifi.authorization.file.generated.Groups;
import org.apache.nifi.authorization.file.generated.Policies;
import org.apache.nifi.authorization.file.generated.Policy;
import org.apache.nifi.authorization.file.generated.Users;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Provides authorizes requests to resources using policies persisted in a file.
 */
public class FileAuthorizer extends AbstractPolicyBasedAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(FileAuthorizer.class);

    private static final String AUTHORIZATIONS_XSD = "/authorizations.xsd";
    private static final String JAXB_AUTHORIZATIONS_PATH = "org.apache.nifi.authorization.file.generated";

    private static final String USERS_XSD = "/users.xsd";
    private static final String JAXB_USERS_PATH = "org.apache.nifi.user.generated";

    private static final String FLOW_XSD = "/FlowConfiguration.xsd";

    private static final JAXBContext JAXB_AUTHORIZATIONS_CONTEXT = initializeJaxbContext(JAXB_AUTHORIZATIONS_PATH);
    private static final JAXBContext JAXB_USERS_CONTEXT = initializeJaxbContext(JAXB_USERS_PATH);

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext(final String contextPath) {
        try {
            return JAXBContext.newInstance(contextPath, FileAuthorizer.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    static final String READ_CODE = "R";
    static final String WRITE_CODE = "W";

    static final String PROP_AUTHORIZATIONS_FILE = "Authorizations File";
    static final String PROP_INITIAL_ADMIN_IDENTITY = "Initial Admin Identity";
    static final String PROP_LEGACY_AUTHORIZED_USERS_FILE = "Legacy Authorized Users File";
    static final Pattern NODE_IDENTITY_PATTERN = Pattern.compile("Node Identity \\S+");

    private Schema flowSchema;
    private Schema usersSchema;
    private Schema authorizationsSchema;
    private SchemaFactory schemaFactory;
    private NiFiProperties properties;
    private File authorizationsFile;
    private File restoreAuthorizationsFile;
    private String rootGroupId;
    private String initialAdminIdentity;
    private String legacyAuthorizedUsersFile;
    private Set<String> nodeIdentities;

    private final AtomicReference<AuthorizationsHolder> authorizationsHolder = new AtomicReference<>();

    @Override
    public void initialize(final AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
        try {
            schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            authorizationsSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(AUTHORIZATIONS_XSD));
            usersSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(USERS_XSD));
            flowSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(FLOW_XSD));
        } catch (Exception e) {
            throw new AuthorizerCreationException(e);
        }
    }

    @Override
    public void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            final PropertyValue authorizationsPath = configurationContext.getProperty(PROP_AUTHORIZATIONS_FILE);
            if (StringUtils.isBlank(authorizationsPath.getValue())) {
                throw new AuthorizerCreationException("The authorizations file must be specified.");
            }

            // get the authorizations file and ensure it exists
            authorizationsFile = new File(authorizationsPath.getValue());
            if (!authorizationsFile.exists()) {
                logger.info("Creating new authorizations file at {}", new Object[] {authorizationsFile.getAbsolutePath()});
                saveAndRefreshHolder(new Authorizations());
            }

            final File authorizationsFileDirectory = authorizationsFile.getAbsoluteFile().getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();
            if (restoreDirectory != null) {
                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the primary directory
                if (authorizationsFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new AuthorizerCreationException(String.format("Authorizations file directory '%s' is the same as restore directory '%s' ",
                            authorizationsFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                // the restore copy will have same file name, but reside in a different directory
                restoreAuthorizationsFile = new File(restoreDirectory, authorizationsFile.getName());

                try {
                    // sync the primary copy with the restore copy
                    FileUtils.syncWithRestore(authorizationsFile, restoreAuthorizationsFile, logger);
                } catch (final IOException | IllegalStateException ioe) {
                    throw new AuthorizerCreationException(ioe);
                }
            }

            // get the value of the initial admin identity
            final PropertyValue initialAdminIdentityProp = configurationContext.getProperty(PROP_INITIAL_ADMIN_IDENTITY);
            initialAdminIdentity = initialAdminIdentityProp == null ? null : initialAdminIdentityProp.getValue();

            // get the value of the legacy authorized users file
            final PropertyValue legacyAuthorizedUsersProp = configurationContext.getProperty(PROP_LEGACY_AUTHORIZED_USERS_FILE);
            legacyAuthorizedUsersFile = legacyAuthorizedUsersProp == null ? null : legacyAuthorizedUsersProp.getValue();

            // extract any node identities
            nodeIdentities = new HashSet<>();
            for (Map.Entry<String,String> entry : configurationContext.getProperties().entrySet()) {
                Matcher matcher = NODE_IDENTITY_PATTERN.matcher(entry.getKey());
                if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                    nodeIdentities.add(entry.getValue());
                }
            }

            // load the authorizations
            load();

            // if we've copied the authorizations file to a restore directory synchronize it
            if (restoreAuthorizationsFile != null) {
                FileUtils.copyFile(authorizationsFile, restoreAuthorizationsFile, false, false, logger);
            }

            logger.info(String.format("Authorizations file loaded at %s", new Date().toString()));

        } catch (IOException | AuthorizerCreationException | JAXBException | IllegalStateException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    /**
     * Loads the authorizations file and populates the AuthorizationsHolder, only called during start-up.
     *
     * @throws JAXBException            Unable to reload the authorized users file
     * @throws IOException              Unable to sync file with restore
     * @throws IllegalStateException    Unable to sync file with restore
     */
    private synchronized void load() throws JAXBException, IOException, IllegalStateException {
        // attempt to unmarshal
        final Unmarshaller unmarshaller = JAXB_AUTHORIZATIONS_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(authorizationsSchema);
        final JAXBElement<Authorizations> element = unmarshaller.unmarshal(new StreamSource(authorizationsFile), Authorizations.class);

        final Authorizations authorizations = element.getValue();

        if (authorizations.getUsers() == null) {
            authorizations.setUsers(new Users());
        }
        if (authorizations.getGroups() == null) {
            authorizations.setGroups(new Groups());
        }
        if (authorizations.getPolicies() == null) {
            authorizations.setPolicies(new Policies());
        }

        final AuthorizationsHolder authorizationsHolder = new AuthorizationsHolder(authorizations);
        final boolean emptyAuthorizations = authorizationsHolder.getAllUsers().isEmpty() && authorizationsHolder.getAllPolicies().isEmpty();
        final boolean hasInitialAdminIdentity = (initialAdminIdentity != null && !StringUtils.isBlank(initialAdminIdentity));
        final boolean hasLegacyAuthorizedUsers = (legacyAuthorizedUsersFile != null && !StringUtils.isBlank(legacyAuthorizedUsersFile));

        // if we are starting fresh then we might need to populate an initial admin or convert legacy users
        if (emptyAuthorizations) {
            // try to extract the root group id from the flow configuration file specified in nifi.properties
            rootGroupId = getRootGroupId();

            if (hasInitialAdminIdentity && hasLegacyAuthorizedUsers) {
                throw new AuthorizerCreationException("Cannot provide an Initial Admin Identity and a Legacy Authorized Users File");
            } else if (hasInitialAdminIdentity) {
                logger.info("Populating authorizations for Initial Admin: " + initialAdminIdentity);
                populateInitialAdmin(authorizations);
            } else if (hasLegacyAuthorizedUsers) {
                logger.info("Converting " + legacyAuthorizedUsersFile + " to new authorizations model");
                convertLegacyAuthorizedUsers(authorizations);
            }

            populateNodes(authorizations);

            // save any changes that were made and repopulate the holder
            saveAndRefreshHolder(authorizations);
        } else {
            this.authorizationsHolder.set(authorizationsHolder);
        }
    }

    /**
     * Extracts the root group id from the flow configuration file provided in nifi.properties.
     *
     * @return the root group id, or null if the files doesn't exist, was empty, or could not be parsed
     */
    private String getRootGroupId() {
        final File flowFile = properties.getFlowConfigurationFile();
        if (flowFile == null) {
            logger.debug("Flow Configuration file was null");
            return null;
        }

        // if the flow doesn't exist or is 0 bytes, then return null
        final Path flowPath = flowFile.toPath();
        try {
            if (!Files.exists(flowPath) || Files.size(flowPath) == 0) {
                logger.debug("Flow Configuration does not exist or was empty");
                return null;
            }
        } catch (IOException e) {
            logger.debug("An error occurred determining the size of the Flow Configuration file");
            return null;
        }

        // otherwise create the appropriate input streams to read the file
        try (final InputStream in = Files.newInputStream(flowPath, StandardOpenOption.READ);
             final InputStream gzipIn = new GZIPInputStream(in)) {

            final byte[] flowBytes = IOUtils.toByteArray(gzipIn);
            if (flowBytes == null || flowBytes.length == 0) {
                logger.debug("Could not extract root group id because Flow Configuration File was empty");
                return null;
            }

            // create validating document builder
            final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            docFactory.setSchema(flowSchema);

            // parse the flow
            final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            final Document document = docBuilder.parse(new ByteArrayInputStream(flowBytes));

            // extract the root group id
            final Element rootElement = document.getDocumentElement();

            final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
            if (rootGroupElement == null) {
                logger.debug("rootGroup element not found in Flow Configuration file");
                return null;
            }

            final Element rootGroupIdElement = (Element) rootGroupElement.getElementsByTagName("id").item(0);
            if (rootGroupIdElement == null) {
                logger.debug("id element not found under rootGroup in Flow Configuration file");
                return null;
            }

            return rootGroupIdElement.getTextContent();

        } catch (final SAXException | ParserConfigurationException | IOException ex) {
            logger.error("Unable to find root group id in {} due to {}", new Object[] { flowPath.toAbsolutePath(), ex });
            return null;
        }
    }

    /**
     *  Creates the initial admin user and policies for access the flow and managing users and policies.
     */
    private void populateInitialAdmin(final Authorizations authorizations) {
        // generate an identifier and add a User with the given identifier and identity
        final UUID adminIdentifier = UUID.nameUUIDFromBytes(initialAdminIdentity.getBytes(StandardCharsets.UTF_8));
        final User adminUser = new User.Builder().identifier(adminIdentifier.toString()).identity(initialAdminIdentity).build();

        final org.apache.nifi.authorization.file.generated.User jaxbAdminUser = createJAXBUser(adminUser);
        authorizations.getUsers().getUser().add(jaxbAdminUser);

        // grant the user read access to the /flow resource
        addAccessPolicy(authorizations, ResourceType.Flow.getValue(), adminUser.getIdentifier(), READ_CODE);

        // grant the user read access to the root process group resource
        if (rootGroupId != null) {
            addAccessPolicy(authorizations, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, adminUser.getIdentifier(), READ_CODE);
            addAccessPolicy(authorizations, ResourceType.ProcessGroup.getValue() + "/" + rootGroupId, adminUser.getIdentifier(), WRITE_CODE);
        }

        // grant the user read/write access to the /tenants resource
        addAccessPolicy(authorizations, ResourceType.Tenant.getValue(), adminUser.getIdentifier(), READ_CODE);
        addAccessPolicy(authorizations, ResourceType.Tenant.getValue(), adminUser.getIdentifier(), WRITE_CODE);

        // grant the user read/write access to the /policies resource
        addAccessPolicy(authorizations, ResourceType.Policy.getValue(), adminUser.getIdentifier(), READ_CODE);
        addAccessPolicy(authorizations, ResourceType.Policy.getValue(), adminUser.getIdentifier(), WRITE_CODE);
    }

    /**
     * Creates a user for each node and gives the nodes write permission to /proxy.
     *
     * @param authorizations the overall authorizations
     */
    private void populateNodes(Authorizations authorizations) {
        for (String nodeIdentity : nodeIdentities) {
            // see if we have an existing user for the given node identity
            org.apache.nifi.authorization.file.generated.User jaxbNodeUser = null;
            for (org.apache.nifi.authorization.file.generated.User user : authorizations.getUsers().getUser()) {
                if (user.getIdentity().equals(nodeIdentity)) {
                    jaxbNodeUser = user;
                    break;
                }
            }

            // if we didn't find an existing user then create a new one
            if (jaxbNodeUser == null) {
                // generate an identifier and add a User with the given identifier and identity
                final UUID nodeIdentifier = UUID.nameUUIDFromBytes(nodeIdentity.getBytes(StandardCharsets.UTF_8));
                final User nodeUser = new User.Builder().identifier(nodeIdentifier.toString()).identity(nodeIdentity).build();

                jaxbNodeUser = createJAXBUser(nodeUser);
                authorizations.getUsers().getUser().add(jaxbNodeUser);
            }

            // grant access to the proxy resource
            addAccessPolicy(authorizations, ResourceType.Proxy.getValue(), jaxbNodeUser.getIdentifier(), READ_CODE);
            addAccessPolicy(authorizations, ResourceType.Proxy.getValue(), jaxbNodeUser.getIdentifier(), WRITE_CODE);
        }
    }

    /**
     * Unmarshalls an existing authorized-users.xml and converts the object model to the new model.
     *
     * @param authorizations the current Authorizations instance that users, groups, and policies will be added to
     * @throws AuthorizerCreationException if the legacy authorized users file that was provided does not exist
     * @throws JAXBException if the legacy authorized users file that was provided could not be unmarshalled
     */
    private void convertLegacyAuthorizedUsers(final Authorizations authorizations) throws AuthorizerCreationException, JAXBException {
        final File authorizedUsersFile = new File(legacyAuthorizedUsersFile);
        if (!authorizedUsersFile.exists()) {
            throw new AuthorizerCreationException("Legacy Authorized Users File '" + legacyAuthorizedUsersFile + "' does not exists");
        }

        final Unmarshaller unmarshaller = JAXB_USERS_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(usersSchema);

        final JAXBElement<org.apache.nifi.user.generated.Users> element = unmarshaller.unmarshal(
                new StreamSource(authorizedUsersFile), org.apache.nifi.user.generated.Users.class);

        final org.apache.nifi.user.generated.Users users = element.getValue();
        if (users.getUser().isEmpty()) {
            logger.info("Legacy Authorized Users File contained no users, nothing to convert");
            return;
        }

        // get all the user DNs into a list
        List<String> userIdentities = new ArrayList<>();
        for (org.apache.nifi.user.generated.User legacyUser : users.getUser()) {
            userIdentities.add(legacyUser.getDn());
        }

        // sort the list and pull out the first identity
        Collections.sort(userIdentities);
        final String seedIdentity = userIdentities.get(0);

        // create mapping from Role to access policies
        final Map<Role,Set<RoleAccessPolicy>> roleAccessPolicies = RoleAccessPolicy.getMappings(rootGroupId);

        final List<Policy> allPolicies = new ArrayList<>();

        for (org.apache.nifi.user.generated.User legacyUser : users.getUser()) {
            // create the identifier of the new user based on the DN
            String legacyUserDn = legacyUser.getDn();
            String userIdentifier = UUID.nameUUIDFromBytes(legacyUserDn.getBytes(StandardCharsets.UTF_8)).toString();

            // create the new User and add it to the list of users
            org.apache.nifi.authorization.file.generated.User user = new org.apache.nifi.authorization.file.generated.User();
            user.setIdentifier(userIdentifier);
            user.setIdentity(legacyUserDn);
            authorizations.getUsers().getUser().add(user);

            // if there was a group name find or create the group and add the user to it
            org.apache.nifi.authorization.file.generated.Group group = getOrCreateGroup(authorizations, legacyUser.getGroup());
            if (group != null) {
                org.apache.nifi.authorization.file.generated.Group.User groupUser = new org.apache.nifi.authorization.file.generated.Group.User();
                groupUser.setIdentifier(userIdentifier);
                group.getUser().add(groupUser);
            }

            // create policies based on the given role
            for (org.apache.nifi.user.generated.Role jaxbRole : legacyUser.getRole()) {
                Role role = Role.valueOf(jaxbRole.getName());
                Set<RoleAccessPolicy> policies = roleAccessPolicies.get(role);

                for (RoleAccessPolicy roleAccessPolicy : policies) {

                    // get the matching policy, or create a new one
                    Policy policy = getOrCreatePolicy(
                            allPolicies,
                            seedIdentity,
                            roleAccessPolicy.getResource(),
                            roleAccessPolicy.getAction());

                    // determine if the user already exists in the policy
                    boolean userExists = false;
                    for (Policy.User policyUser : policy.getUser()) {
                        if (policyUser.getIdentifier().equals(userIdentifier)) {
                            userExists = true;
                            break;
                        }
                    }

                    // add the user to the policy if doesn't already exist
                    if (!userExists) {
                        Policy.User policyUser = new Policy.User();
                        policyUser.setIdentifier(userIdentifier);
                        policy.getUser().add(policyUser);
                    }
                }
            }

        }

        authorizations.getPolicies().getPolicy().addAll(allPolicies);
    }

    /**
     * Finds the Group with the given name, or creates a new one and adds it to Authorizations.
     *
     * @param authorizations the Authorizations reference
     * @param groupName the name of the group to look for
     * @return the Group from Authorizations with the given name, or a new instance
     */
    private org.apache.nifi.authorization.file.generated.Group getOrCreateGroup(final Authorizations authorizations, final String groupName) {
        if (StringUtils.isBlank(groupName)) {
            return null;
        }

        org.apache.nifi.authorization.file.generated.Group foundGroup = null;
        for (org.apache.nifi.authorization.file.generated.Group group : authorizations.getGroups().getGroup()) {
            if (group.getName().equals(groupName)) {
                foundGroup = group;
                break;
            }
        }

        if (foundGroup == null) {
            UUID newGroupIdentifier = UUID.nameUUIDFromBytes(groupName.getBytes(StandardCharsets.UTF_8));
            foundGroup = new org.apache.nifi.authorization.file.generated.Group();
            foundGroup.setIdentifier(newGroupIdentifier.toString());
            foundGroup.setName(groupName);
            authorizations.getGroups().getGroup().add(foundGroup);
        }

        return foundGroup;
    }

    /**
     * Finds the Policy matching the resource and action, or creates a new one and adds it to the list of policies.
     *
     * @param policies the policies to search through
     * @param seedIdentity the seedIdentity to use when creating identifiers for new policies
     * @param resource the resource for the policy
     * @param action the action string for the police (R or RW)
     * @return the matching policy or a new policy
     */
    private Policy getOrCreatePolicy(final List<Policy> policies, final String seedIdentity, final String resource, final String action) {
        Policy foundPolicy = null;

        // try to find a policy with the same resource and actions
        for (Policy policy : policies) {
            if (policy.getResource().equals(resource) && policy.getAction().equals(action)) {
                foundPolicy = policy;
                break;
            }
        }

        // if a matching policy wasn't found then create one
        if (foundPolicy == null) {
            final String uuidSeed = resource + action + seedIdentity;
            final UUID policyIdentifier = UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8));

            foundPolicy = new Policy();
            foundPolicy.setIdentifier(policyIdentifier.toString());
            foundPolicy.setResource(resource);
            foundPolicy.setAction(action);

            policies.add(foundPolicy);
        }

        return foundPolicy;
    }

    /**
     * Creates and adds an access policy for the given resource, identity, and actions.
     *
     * @param authorizations the Authorizations instance to add the policy to
     * @param resource the resource for the policy
     * @param identity the identity for the policy
     * @param action the action for the policy
     */
    private void addAccessPolicy(final Authorizations authorizations, final String resource, final String identity, final String action) {
        // first try to find an existing policy for the given resource and action
        Policy foundPolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getResource().equals(resource) && policy.getAction().equals(action)) {
                foundPolicy = policy;
                break;
            }
        }

        if (foundPolicy == null) {
            // if we didn't find an existing policy create a new one
            final String uuidSeed = resource + identity + action;
            final UUID policyIdentifier = UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8));

            final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                    .identifier(policyIdentifier.toString())
                    .resource(resource)
                    .addUser(identity);

            if (action.equals(READ_CODE)) {
                builder.action(RequestAction.READ);
            } else if (action.equals(WRITE_CODE)) {
                builder.action(RequestAction.WRITE);
            } else {
                throw new IllegalStateException("Unknown Policy Action: " + action);
            }

            final AccessPolicy accessPolicy = builder.build();
            final Policy jaxbPolicy = createJAXBPolicy(accessPolicy);
            authorizations.getPolicies().getPolicy().add(jaxbPolicy);
        } else {
            // otherwise add the user to the existing policy
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(identity);
            foundPolicy.getUser().add(policyUser);
        }
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
     *
     * Synchronized to ensure only one thread writes the file at a time.
     *
     * @param authorizations the authorizations to save and populate from
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private synchronized void saveAndRefreshHolder(final Authorizations authorizations) throws AuthorizationAccessException {
        try {
            final Marshaller marshaller = JAXB_AUTHORIZATIONS_CONTEXT.createMarshaller();
            marshaller.setSchema(authorizationsSchema);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(authorizations, authorizationsFile);

            final AuthorizationsHolder authorizationsHolder = new AuthorizationsHolder(authorizations);
            this.authorizationsHolder.set(authorizationsHolder);
        } catch (JAXBException e) {
            throw new AuthorizationAccessException("Unable to save Authorizations", e);
        }
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public void preDestruction() {

    }

    // ------------------ Groups ------------------

    @Override
    public synchronized Group addGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();

        // determine that all users in the group exist before doing anything, throw an exception if they don't
        final Set<org.apache.nifi.authorization.file.generated.User> jaxbUsers = checkGroupUsers(group, authorizations.getUsers().getUser());

        // create a new JAXB Group based on the incoming Group
        final org.apache.nifi.authorization.file.generated.Group jaxbGroup = new org.apache.nifi.authorization.file.generated.Group();
        jaxbGroup.setIdentifier(group.getIdentifier());
        jaxbGroup.setName(group.getName());

        // add each user to the group
        for (String groupUser : group.getUsers()) {
            org.apache.nifi.authorization.file.generated.Group.User jaxbGroupUser = new org.apache.nifi.authorization.file.generated.Group.User();
            jaxbGroupUser.setIdentifier(groupUser);
            jaxbGroup.getUser().add(jaxbGroupUser);
        }

        authorizations.getGroups().getGroup().add(jaxbGroup);
        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        return holder.getGroupsById().get(group.getIdentifier());
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }
        return authorizationsHolder.get().getGroupsById().get(identifier);
    }

    @Override
    public synchronized Group updateGroup(Group group) throws AuthorizationAccessException {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();

        // find the group that needs to be update
        org.apache.nifi.authorization.file.generated.Group updateGroup = null;
        for (org.apache.nifi.authorization.file.generated.Group jaxbGroup : authorizations.getGroups().getGroup()) {
            if (jaxbGroup.getIdentifier().equals(group.getIdentifier())) {
                updateGroup = jaxbGroup;
                break;
            }
        }

        // if the group wasn't found return null, otherwise update the group and save changes
        if (updateGroup == null) {
            return null;
        }

        // reset the list of users and add each user to the group
        updateGroup.getUser().clear();
        for (String groupUser : group.getUsers()) {
            org.apache.nifi.authorization.file.generated.Group.User jaxbGroupUser = new org.apache.nifi.authorization.file.generated.Group.User();
            jaxbGroupUser.setIdentifier(groupUser);
            updateGroup.getUser().add(jaxbGroupUser);
        }

        updateGroup.setName(group.getName());
        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        return holder.getGroupsById().get(group.getIdentifier());
    }

    @Override
    public synchronized Group deleteGroup(Group group) throws AuthorizationAccessException {
        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        final List<org.apache.nifi.authorization.file.generated.Group> groups = authorizations.getGroups().getGroup();

        // for each policy iterate over the group reference and remove the group reference if it matches the group being deleted
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            Iterator<Policy.Group> policyGroupIter = policy.getGroup().iterator();
            while (policyGroupIter.hasNext()) {
                Policy.Group policyGroup = policyGroupIter.next();
                if (policyGroup.getIdentifier().equals(group.getIdentifier())) {
                    policyGroupIter.remove();
                    break;
                }
            }
        }

        // now remove the actual group from the top-level list of groups
        boolean removedGroup = false;
        Iterator<org.apache.nifi.authorization.file.generated.Group> iter = groups.iterator();
        while (iter.hasNext()) {
            org.apache.nifi.authorization.file.generated.Group jaxbGroup = iter.next();
            if (group.getIdentifier().equals(jaxbGroup.getIdentifier())) {
                iter.remove();
                removedGroup = true;
                break;
            }
        }

        if (removedGroup) {
            saveAndRefreshHolder(authorizations);
            return group;
        } else {
            return null;
        }
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllGroups();
    }

    private Set<org.apache.nifi.authorization.file.generated.User> checkGroupUsers(final Group group, final List<org.apache.nifi.authorization.file.generated.User> users) {
        final Set<org.apache.nifi.authorization.file.generated.User> jaxbUsers = new HashSet<>();
        for (String groupUser : group.getUsers()) {
            boolean found = false;
            for (org.apache.nifi.authorization.file.generated.User jaxbUser : users) {
                if (jaxbUser.getIdentifier().equals(groupUser)) {
                    jaxbUsers.add(jaxbUser);
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new IllegalStateException("Unable to add group because user " + groupUser + " does not exist");
            }
        }
        return jaxbUsers;
    }

    // ------------------ Users ------------------

    @Override
    public synchronized User addUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final org.apache.nifi.authorization.file.generated.User jaxbUser = createJAXBUser(user);

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        authorizations.getUsers().getUser().add(jaxbUser);

        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getUsersById().get(user.getIdentifier());
    }

    private org.apache.nifi.authorization.file.generated.User createJAXBUser(User user) {
        final org.apache.nifi.authorization.file.generated.User jaxbUser = new org.apache.nifi.authorization.file.generated.User();
        jaxbUser.setIdentifier(user.getIdentifier());
        jaxbUser.setIdentity(user.getIdentity());
        return jaxbUser;
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getUsersById().get(identifier);
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        if (identity == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getUsersByIdentity().get(identity);
    }

    @Override
    public synchronized User updateUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        final List<org.apache.nifi.authorization.file.generated.User> users = authorizations.getUsers().getUser();

        // fine the User that needs to be updated
        org.apache.nifi.authorization.file.generated.User updateUser = null;
        for (org.apache.nifi.authorization.file.generated.User jaxbUser : users) {
            if (user.getIdentifier().equals(jaxbUser.getIdentifier())) {
                updateUser = jaxbUser;
                break;
            }
        }

        // if user wasn't found return null, otherwise update the user and save changes
        if (updateUser == null) {
            return null;
        } else {
            updateUser.setIdentity(user.getIdentity());
            saveAndRefreshHolder(authorizations);

            final AuthorizationsHolder holder = authorizationsHolder.get();
            return holder.getUsersById().get(user.getIdentifier());
        }
    }

    @Override
    public synchronized User deleteUser(final User user) throws AuthorizationAccessException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        final List<org.apache.nifi.authorization.file.generated.User> users = authorizations.getUsers().getUser();

        // for each group iterate over the user references and remove the user reference if it matches the user being deleted
        for (org.apache.nifi.authorization.file.generated.Group group : authorizations.getGroups().getGroup()) {
            Iterator<org.apache.nifi.authorization.file.generated.Group.User> groupUserIter = group.getUser().iterator();
            while (groupUserIter.hasNext()) {
                org.apache.nifi.authorization.file.generated.Group.User groupUser = groupUserIter.next();
                if (groupUser.getIdentifier().equals(user.getIdentifier())) {
                    groupUserIter.remove();
                    break;
                }
            }
        }

        // remove any references to the user being deleted from policies
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            Iterator<Policy.User> policyUserIter = policy.getUser().iterator();
            while (policyUserIter.hasNext()) {
                Policy.User policyUser = policyUserIter.next();
                if (policyUser.getIdentifier().equals(user.getIdentifier())) {
                    policyUserIter.remove();
                    break;
                }
            }
        }

        // remove the actual user if it exists
        boolean removedUser = false;
        Iterator<org.apache.nifi.authorization.file.generated.User> iter = users.iterator();
        while (iter.hasNext()) {
            org.apache.nifi.authorization.file.generated.User jaxbUser = iter.next();
            if (user.getIdentifier().equals(jaxbUser.getIdentifier())) {
                iter.remove();
                removedUser = true;
                break;
            }
        }

        if (removedUser) {
            saveAndRefreshHolder(authorizations);
            return user;
        } else {
            return null;
        }
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllUsers();
    }

    // ------------------ AccessPolicies ------------------

    @Override
    public synchronized AccessPolicy doAddAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        // create the new JAXB Policy
        final Policy policy = createJAXBPolicy(accessPolicy);

        // add the new Policy to the top-level list of policies
        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();
        authorizations.getPolicies().getPolicy().add(policy);

        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(accessPolicy.getIdentifier());
    }

    private Policy createJAXBPolicy(final AccessPolicy accessPolicy) {
        final Policy policy = new Policy();
        policy.setIdentifier(accessPolicy.getIdentifier());
        policy.setResource(accessPolicy.getResource());

        switch (accessPolicy.getAction()) {
            case READ:
                policy.setAction(READ_CODE);
                break;
            case WRITE:
                policy.setAction(WRITE_CODE);
                break;
            default:
                break;
        }

        transferUsersAndGroups(accessPolicy, policy);
        return policy;
    }

    @Override
    public AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(identifier);
    }

    @Override
    public synchronized AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();

        // try to find an existing Authorization that matches the policy id
        Policy updatePolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                updatePolicy = policy;
                break;
            }
        }

        // no matching Policy so return null
        if (updatePolicy == null) {
            return null;
        }

        // update the Policy, save, reload, and return
        transferUsersAndGroups(accessPolicy, updatePolicy);
        saveAndRefreshHolder(authorizations);

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(accessPolicy.getIdentifier());
    }

    @Override
    public synchronized AccessPolicy deleteAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final Authorizations authorizations = this.authorizationsHolder.get().getAuthorizations();

        // find the matching Policy and remove it
        boolean deletedPolicy = false;
        Iterator<Policy> policyIter = authorizations.getPolicies().getPolicy().iterator();
        while (policyIter.hasNext()) {
            final Policy policy = policyIter.next();
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                policyIter.remove();
                deletedPolicy = true;
                break;
            }
        }

        // never found a matching Policy so return null
        if (!deletedPolicy) {
            return null;
        }

        saveAndRefreshHolder(authorizations);
        return accessPolicy;
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllPolicies();
    }

    @Override
    public UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException {
        return authorizationsHolder.get();
    }

    /**
     * Sets the given Policy to the state of the provided AccessPolicy. Users and Groups will be cleared and
     * set to match the AccessPolicy, the resource and action will be set to match the AccessPolicy.
     *
     * Does not set the identifier.
     *
     * @param accessPolicy the AccessPolicy to transfer state from
     * @param policy the Policy to transfer state to
     */
    private void transferUsersAndGroups(AccessPolicy accessPolicy, Policy policy) {
        // add users to the policy
        policy.getUser().clear();
        for (String userIdentifier : accessPolicy.getUsers()) {
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(userIdentifier);
            policy.getUser().add(policyUser);
        }

        // add groups to the policy
        policy.getGroup().clear();
        for (String groupIdentifier : accessPolicy.getGroups()) {
            Policy.Group policyGroup = new Policy.Group();
            policyGroup.setIdentifier(groupIdentifier);
            policy.getGroup().add(policyGroup);
        }
    }

}
