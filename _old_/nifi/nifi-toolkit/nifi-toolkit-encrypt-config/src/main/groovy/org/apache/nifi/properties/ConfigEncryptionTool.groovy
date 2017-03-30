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
package org.apache.nifi.properties

import groovy.io.GroovyPrintWriter
import groovy.xml.XmlUtil
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.codec.binary.Hex
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException
import org.apache.nifi.toolkit.tls.commandLine.ExitCode
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.console.TextDevice
import org.apache.nifi.util.console.TextDevices
import org.bouncycastle.crypto.generators.SCrypt
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.nio.charset.StandardCharsets
import java.security.KeyException
import java.security.Security

class ConfigEncryptionTool {
    private static final Logger logger = LoggerFactory.getLogger(ConfigEncryptionTool.class)

    public String bootstrapConfPath
    public String niFiPropertiesPath
    public String outputNiFiPropertiesPath
    public String loginIdentityProvidersPath
    public String outputLoginIdentityProvidersPath

    private String keyHex
    private String migrationKeyHex
    private String password
    private String migrationPassword

    private NiFiProperties niFiProperties
    private String loginIdentityProviders

    private boolean usingPassword = true
    private boolean usingPasswordMigration = true
    private boolean migration = false
    private boolean isVerbose = false
    private boolean handlingNiFiProperties = false
    private boolean handlingLoginIdentityProviders = false

    private static final String HELP_ARG = "help"
    private static final String VERBOSE_ARG = "verbose"
    private static final String BOOTSTRAP_CONF_ARG = "bootstrapConf"
    private static final String NIFI_PROPERTIES_ARG = "niFiProperties"
    private static final String LOGIN_IDENTITY_PROVIDERS_ARG = "loginIdentityProviders"
    private static final String OUTPUT_NIFI_PROPERTIES_ARG = "outputNiFiProperties"
    private static final String OUTPUT_LOGIN_IDENTITY_PROVIDERS_ARG = "outputLoginIdentityProviders"
    private static final String KEY_ARG = "key"
    private static final String PASSWORD_ARG = "password"
    private static final String KEY_MIGRATION_ARG = "oldKey"
    private static final String PASSWORD_MIGRATION_ARG = "oldPassword"
    private static final String USE_KEY_ARG = "useRawKey"
    private static final String MIGRATION_ARG = "migrate"

    private static final int MIN_PASSWORD_LENGTH = 12

    // Strong parameters as of 12 Aug 2016
    private static final int SCRYPT_N = 2**16
    private static final int SCRYPT_R = 8
    private static final int SCRYPT_P = 1

    private static
    final String BOOTSTRAP_KEY_COMMENT = "# Master key in hexadecimal format for encrypted sensitive configuration values"
    private static final String BOOTSTRAP_KEY_PREFIX = "nifi.bootstrap.sensitive.key="
    private static final String JAVA_HOME = "JAVA_HOME"
    private static final String NIFI_TOOLKIT_HOME = "NIFI_TOOLKIT_HOME"
    private static final String SEP = System.lineSeparator()

    private static final String FOOTER = buildFooter()

    private static
    final String DEFAULT_DESCRIPTION = "This tool reads from a nifi.properties and/or login-identity-providers.xml file with plain sensitive configuration values, prompts the user for a master key, and encrypts each value. It will replace the plain value with the protected value in the same file (or write to a new file if specified)."
    static private final String LDAP_PROVIDER_REGEX = /<provider>\s*<identifier>\s*ldap-provider[\s\S]*?<\/provider>/
    static private final String XML_DECLARATION_REGEX = /<\?xml version="1.0" encoding="UTF-8"\?>/

    private static String buildHeader(String description = DEFAULT_DESCRIPTION) {
        "${SEP}${description}${SEP * 2}"
    }

    private static String buildFooter() {
        "${SEP}Java home: ${System.getenv(JAVA_HOME)}${SEP}NiFi Toolkit home: ${System.getenv(NIFI_TOOLKIT_HOME)}"
    }

    private final Options options;
    private final String header;


    public ConfigEncryptionTool() {
        this(DEFAULT_DESCRIPTION)
    }

    public ConfigEncryptionTool(String description) {
        this.header = buildHeader(description)
        this.options = new Options()
        options.addOption("h", HELP_ARG, false, "Prints this usage message")
        options.addOption("v", VERBOSE_ARG, false, "Sets verbose mode (default false)")
        options.addOption("n", NIFI_PROPERTIES_ARG, true, "The nifi.properties file containing unprotected config values (will be overwritten)")
        options.addOption("l", LOGIN_IDENTITY_PROVIDERS_ARG, true, "The login-identity-providers.xml file containing unprotected config values (will be overwritten)")
        options.addOption("b", BOOTSTRAP_CONF_ARG, true, "The bootstrap.conf file to persist master key")
        options.addOption("o", OUTPUT_NIFI_PROPERTIES_ARG, true, "The destination nifi.properties file containing protected config values (will not modify input nifi.properties)")
        options.addOption("i", OUTPUT_LOGIN_IDENTITY_PROVIDERS_ARG, true, "The destination login-identity-providers.xml file containing protected config values (will not modify input login-identity-providers.xml)")
        options.addOption("k", KEY_ARG, true, "The raw hexadecimal key to use to encrypt the sensitive properties")
        options.addOption("e", KEY_MIGRATION_ARG, true, "The old raw hexadecimal key to use during key migration")
        options.addOption("p", PASSWORD_ARG, true, "The password from which to derive the key to use to encrypt the sensitive properties")
        options.addOption("w", PASSWORD_MIGRATION_ARG, true, "The old password from which to derive the key during migration")
        options.addOption("r", USE_KEY_ARG, false, "If provided, the secure console will prompt for the raw key value in hexadecimal form")
        options.addOption("m", MIGRATION_ARG, false, "If provided, the sensitive properties will be re-encrypted with a new key")
    }

    /**
     * Prints the usage message and available arguments for this tool (along with a specific error message if provided).
     *
     * @param errorMessage the optional error message
     */
    public void printUsage(String errorMessage) {
        if (errorMessage) {
            System.out.println(errorMessage)
            System.out.println()
        }
        HelpFormatter helpFormatter = new HelpFormatter()
        helpFormatter.setWidth(160)
        helpFormatter.printHelp(ConfigEncryptionTool.class.getCanonicalName(), header, options, FOOTER, true)
    }

    protected void printUsageAndThrow(String errorMessage, ExitCode exitCode) throws CommandLineParseException {
        printUsage(errorMessage);
        throw new CommandLineParseException(errorMessage, exitCode);
    }

    // TODO: Refactor component steps into methods
    protected CommandLine parse(String[] args) throws CommandLineParseException {
        CommandLineParser parser = new DefaultParser()
        CommandLine commandLine
        try {
            commandLine = parser.parse(options, args)
            if (commandLine.hasOption(HELP_ARG)) {
                printUsageAndThrow(null, ExitCode.HELP)
            }

            isVerbose = commandLine.hasOption(VERBOSE_ARG)

            bootstrapConfPath = commandLine.getOptionValue(BOOTSTRAP_CONF_ARG)

            if (commandLine.hasOption(NIFI_PROPERTIES_ARG)) {
                if (isVerbose) {
                    logger.info("Handling encryption of nifi.properties")
                }
                niFiPropertiesPath = commandLine.getOptionValue(NIFI_PROPERTIES_ARG)
                outputNiFiPropertiesPath = commandLine.getOptionValue(OUTPUT_NIFI_PROPERTIES_ARG, niFiPropertiesPath)
                handlingNiFiProperties = true

                if (niFiPropertiesPath == outputNiFiPropertiesPath) {
                    // TODO: Add confirmation pause and provide -y flag to offer no-interaction mode?
                    logger.warn("The source nifi.properties and destination nifi.properties are identical [${outputNiFiPropertiesPath}] so the original will be overwritten")
                }
            }

            if (commandLine.hasOption(LOGIN_IDENTITY_PROVIDERS_ARG)) {
                if (isVerbose) {
                    logger.info("Handling encryption of login-identity-providers.xml")
                }
                loginIdentityProvidersPath = commandLine.getOptionValue(LOGIN_IDENTITY_PROVIDERS_ARG)
                outputLoginIdentityProvidersPath = commandLine.getOptionValue(OUTPUT_LOGIN_IDENTITY_PROVIDERS_ARG, loginIdentityProvidersPath)
                handlingLoginIdentityProviders = true

                if (loginIdentityProvidersPath == outputLoginIdentityProvidersPath) {
                    // TODO: Add confirmation pause and provide -y flag to offer no-interaction mode?
                    logger.warn("The source login-identity-providers.xml and destination login-identity-providers.xml are identical [${outputLoginIdentityProvidersPath}] so the original will be overwritten")
                }
            }

            if (isVerbose) {
                logger.info("       bootstrap.conf:               \t${bootstrapConfPath}")
                logger.info("(src)  nifi.properties:              \t${niFiPropertiesPath}")
                logger.info("(dest) nifi.properties:              \t${outputNiFiPropertiesPath}")
                logger.info("(src)  login-identity-providers.xml: \t${loginIdentityProvidersPath}")
                logger.info("(dest) login-identity-providers.xml: \t${outputLoginIdentityProvidersPath}")
            }

            // TODO: Implement in NIFI-2655
//            if (!commandLine.hasOption(NIFI_PROPERTIES_ARG) && !commandLine.hasOption(LOGIN_IDENTITY_PROVIDERS_ARG)) {
//                printUsageAndThrow("One of '-n'/'--${NIFI_PROPERTIES_ARG}' or '-l'/'--${LOGIN_IDENTITY_PROVIDERS_ARG}' must be provided", ExitCode.INVALID_ARGS)
//            }

            if (commandLine.hasOption(MIGRATION_ARG)) {
                migration = true
                if (isVerbose) {
                    logger.info("Key migration mode activated")
                }
                if (commandLine.hasOption(PASSWORD_MIGRATION_ARG)) {
                    usingPasswordMigration = true
                    if (commandLine.hasOption(KEY_MIGRATION_ARG)) {
                        printUsageAndThrow("Only one of '-w'/'--${PASSWORD_MIGRATION_ARG}' and '-e'/'--${KEY_MIGRATION_ARG}' can be used", ExitCode.INVALID_ARGS)
                    } else {
                        migrationPassword = commandLine.getOptionValue(PASSWORD_MIGRATION_ARG)
                    }
                } else {
                    migrationKeyHex = commandLine.getOptionValue(KEY_MIGRATION_ARG)
                    usingPasswordMigration = !migrationKeyHex
                }
            } else {
                if (commandLine.hasOption(PASSWORD_MIGRATION_ARG) || commandLine.hasOption(KEY_MIGRATION_ARG)) {
                    printUsageAndThrow("'-w'/'--${PASSWORD_MIGRATION_ARG}' and '-e'/'--${KEY_MIGRATION_ARG}' are ignored unless '-m'/'--${MIGRATION_ARG}' is enabled", ExitCode.INVALID_ARGS)
                }
            }

            if (commandLine.hasOption(PASSWORD_ARG)) {
                usingPassword = true
                if (commandLine.hasOption(KEY_ARG)) {
                    printUsageAndThrow("Only one of '-p'/'--${PASSWORD_ARG}' and '-k'/'--${KEY_ARG}' can be used", ExitCode.INVALID_ARGS)
                } else {
                    password = commandLine.getOptionValue(PASSWORD_ARG)
                }
            } else {
                keyHex = commandLine.getOptionValue(KEY_ARG)
                usingPassword = !keyHex
            }

            if (commandLine.hasOption(USE_KEY_ARG)) {
                if (keyHex || password) {
                    logger.warn("If the key or password is provided in the arguments, '-r'/'--${USE_KEY_ARG}' is ignored")
                } else {
                    usingPassword = false
                }
            }
        } catch (ParseException e) {
            if (isVerbose) {
                logger.error("Encountered an error", e)
            }
            printUsageAndThrow("Error parsing command line. (" + e.getMessage() + ")", ExitCode.ERROR_PARSING_COMMAND_LINE)
        }
        return commandLine
    }

    /**
     * The method returns the provided, derived, or securely-entered key in hex format. The reason the parameters must be provided instead of read from the fields is because this is used for the regular key/password and the migration key/password.
     *
     * @param device
     * @param keyHex
     * @param password
     * @param usingPassword
     * @return
     */
    private String getKeyInternal(TextDevice device = TextDevices.defaultTextDevice(), String keyHex, String password, boolean usingPassword) {
        if (usingPassword) {
            if (!password) {
                if (isVerbose) {
                    logger.info("Reading password from secure console")
                }
                password = readPasswordFromConsole(device)
            }
            keyHex = deriveKeyFromPassword(password)
            password = null
            return keyHex
        } else {
            if (!keyHex) {
                if (isVerbose) {
                    logger.info("Reading hex key from secure console")
                }
                keyHex = readKeyFromConsole(device)
            }
            return keyHex
        }
    }

    private String getKey(TextDevice textDevice = TextDevices.defaultTextDevice()) {
        getKeyInternal(textDevice, keyHex, password, usingPassword)
    }

    private String getMigrationKey() {
        getKeyInternal(TextDevices.defaultTextDevice(), migrationKeyHex, migrationPassword, usingPasswordMigration)
    }

    private static String readKeyFromConsole(TextDevice textDevice) {
        textDevice.printf("Enter the master key in hexadecimal format (spaces acceptable): ")
        new String(textDevice.readPassword())
    }

    private static String readPasswordFromConsole(TextDevice textDevice) {
        textDevice.printf("Enter the password: ")
        new String(textDevice.readPassword())
    }

    /**
     * Returns the key in uppercase hexadecimal format with delimiters (spaces, '-', etc.) removed. All non-hex chars are removed. If the result is not a valid length (32, 48, 64 chars depending on the JCE), an exception is thrown.
     *
     * @param rawKey the unprocessed key input
     * @return the formatted hex string in uppercase
     * @throws KeyException if the key is not a valid length after parsing
     */
    private static String parseKey(String rawKey) throws KeyException {
        String hexKey = rawKey.replaceAll("[^0-9a-fA-F]", "")
        def validKeyLengths = getValidKeyLengths()
        if (!validKeyLengths.contains(hexKey.size() * 4)) {
            throw new KeyException("The key (${hexKey.size()} hex chars) must be of length ${validKeyLengths} bits (${validKeyLengths.collect { it / 4 }} hex characters)")
        }
        hexKey.toUpperCase()
    }

    /**
     * Returns the list of acceptable key lengths in bits based on the current JCE policies.
     *
     * @return 128 , [192, 256]
     */
    public static List<Integer> getValidKeyLengths() {
        Cipher.getMaxAllowedKeyLength("AES") > 128 ? [128, 192, 256] : [128]
    }

    /**
     * Loads the {@link NiFiProperties} instance from the provided file path (restoring the original value of the System property {@code nifi.properties.file.path} after loading this instance).
     *
     * @return the NiFiProperties instance
     * @throw IOException if the nifi.properties file cannot be read
     */
    private NiFiProperties loadNiFiProperties(String existingKeyHex = keyHex) throws IOException {
        File niFiPropertiesFile
        if (niFiPropertiesPath && (niFiPropertiesFile = new File(niFiPropertiesPath)).exists()) {
            NiFiProperties properties
            try {
                properties = NiFiPropertiesLoader.withKey(existingKeyHex).load(niFiPropertiesFile)
                logger.info("Loaded NiFiProperties instance with ${properties.size()} properties")
                return properties
            } catch (RuntimeException e) {
                if (isVerbose) {
                    logger.error("Encountered an error", e)
                }
                throw new IOException("Cannot load NiFiProperties from [${niFiPropertiesPath}]", e)
            }
        } else {
            printUsageAndThrow("Cannot load NiFiProperties from [${niFiPropertiesPath}]", ExitCode.ERROR_READING_NIFI_PROPERTIES)
        }
    }

    /**
     * Loads the login identity providers configuration from the provided file path.
     *
     * @param existingKeyHex the key used to encrypt the configs (defaults to the current key)
     *
     * @return the file content
     * @throw IOException if the login-identity-providers.xml file cannot be read
     */
    private String loadLoginIdentityProviders(String existingKeyHex = keyHex) throws IOException {
        File loginIdentityProvidersFile
        if (loginIdentityProvidersPath && (loginIdentityProvidersFile = new File(loginIdentityProvidersPath)).exists()) {
            try {
                String xmlContent = loginIdentityProvidersFile.text
                List<String> lines = loginIdentityProvidersFile.readLines()
                logger.info("Loaded LoginIdentityProviders content (${lines.size()} lines)")
                String decryptedXmlContent = decryptLoginIdentityProviders(xmlContent, existingKeyHex)
//                String decryptedXmlContent = ConfigEncryptionUtility.decryptLoginIdentityProviders(xmlContent)
                return decryptedXmlContent
            } catch (RuntimeException e) {
                if (isVerbose) {
                    logger.error("Encountered an error", e)
                }
                throw new IOException("Cannot load LoginIdentityProviders from [${loginIdentityProvidersPath}]", e)
            }
        } else {
            printUsageAndThrow("Cannot load LoginIdentityProviders from [${loginIdentityProvidersPath}]", ExitCode.ERROR_READING_NIFI_PROPERTIES)
        }
    }

    String decryptLoginIdentityProviders(String encryptedXml, String existingKeyHex = keyHex) {
        AESSensitivePropertyProvider sensitivePropertyProvider = new AESSensitivePropertyProvider(existingKeyHex)

        try {
            def doc = new XmlSlurper().parseText(encryptedXml)
            def passwords = doc.provider.find { it.identifier == 'ldap-provider' }.property.findAll {
                it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
            }

            if (passwords.isEmpty()) {
                if (isVerbose) {
                    logger.info("No encrypted password property elements found in login-identity-providers.xml")
                }
                return encryptedXml
            }

            passwords.each { password ->
                if (isVerbose) {
                    logger.info("Attempting to decrypt ${password.text()}")
                }
                String decryptedValue = sensitivePropertyProvider.unprotect(password.text().trim())
                password.replaceNode {
                    property(name: password.@name, encryption: "none", decryptedValue)
                }
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.info("Updated XML content: ${updatedXml}")
            updatedXml
        } catch (Exception e) {
            printUsageAndThrow("Cannot decrypt login identity providers XML content", ExitCode.SERVICE_ERROR)
        }
    }

    String encryptLoginIdentityProviders(String plainXml, String newKeyHex = keyHex) {
        AESSensitivePropertyProvider sensitivePropertyProvider = new AESSensitivePropertyProvider(newKeyHex)

        // TODO: Switch to XmlParser & XmlNodePrinter to maintain "empty" element structure
        try {
            def doc = new XmlSlurper().parseText(plainXml)
            // Only operate on un-encrypted passwords
            def passwords = doc.provider.find { it.identifier == 'ldap-provider' }
                    .property.findAll {
                it.@name =~ "Password" && (it.@encryption == "none" || it.@encryption == "") && it.text()
            }

            if (passwords.isEmpty()) {
                if (isVerbose) {
                    logger.info("No unencrypted password property elements found in login-identity-providers.xml")
                }
                return plainXml
            }

            passwords.each { password ->
                if (isVerbose) {
                    logger.info("Attempting to encrypt ${password.name()}")
                }
                String encryptedValue = sensitivePropertyProvider.protect(password.text().trim())
                password.replaceNode {
                    property(name: password.@name, encryption: sensitivePropertyProvider.identifierKey, encryptedValue)
                }
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.info("Updated XML content: ${updatedXml}")
            updatedXml
        } catch (Exception e) {
            if (isVerbose) {
                logger.error("Encountered exception", e)
            }
            printUsageAndThrow("Cannot encrypt login identity providers XML content", ExitCode.SERVICE_ERROR)
        }
    }

    /**
     * Accepts a {@link NiFiProperties} instance, iterates over all non-empty sensitive properties which are not already marked as protected, encrypts them using the master key, and updates the property with the protected value. Additionally, adds a new sibling property {@code x.y.z.protected=aes/gcm/{128,256}} for each indicating the encryption scheme used.
     *
     * @param plainProperties the NiFiProperties instance containing the raw values
     * @return the NiFiProperties containing protected values
     */
    private NiFiProperties encryptSensitiveProperties(NiFiProperties plainProperties) {
        if (!plainProperties) {
            throw new IllegalArgumentException("Cannot encrypt empty NiFiProperties")
        }

        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainProperties)

        List<String> sensitivePropertyKeys = protectedWrapper.getSensitivePropertyKeys()
        if (sensitivePropertyKeys.isEmpty()) {
            logger.info("No sensitive properties to encrypt")
            return plainProperties
        }

        // Holder for encrypted properties and protection schemes
        Properties encryptedProperties = new Properties()

        AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(keyHex)
        protectedWrapper.addSensitivePropertyProvider(spp)

        List<String> keysToSkip = []

        // Iterate over each -- encrypt and add .protected if populated
        sensitivePropertyKeys.each { String key ->
            if (!plainProperties.getProperty(key)) {
                logger.debug("Skipping encryption of ${key} because it is empty")
            } else {
                String protectedValue = spp.protect(plainProperties.getProperty(key))

                // Add the encrypted value
                encryptedProperties.setProperty(key, protectedValue)
                logger.info("Protected ${key} with ${spp.getIdentifierKey()} -> \t${protectedValue}")

                // Add the protection key ("x.y.z.protected" -> "aes/gcm/{128,256}")
                String protectionKey = protectedWrapper.getProtectionKey(key)
                encryptedProperties.setProperty(protectionKey, spp.getIdentifierKey())
                logger.info("Updated protection key ${protectionKey}")

                keysToSkip << key << protectionKey
            }
        }

        // Combine the original raw NiFiProperties and the newly-encrypted properties
        // Memory-wasteful but NiFiProperties are immutable -- no setter available (unless we monkey-patch...)
        Set<String> nonSensitiveKeys = plainProperties.getPropertyKeys() - keysToSkip
        nonSensitiveKeys.each { String key ->
            encryptedProperties.setProperty(key, plainProperties.getProperty(key))
        }
        NiFiProperties mergedProperties = new StandardNiFiProperties(encryptedProperties)
        logger.info("Final result: ${mergedProperties.size()} keys including ${ProtectedNiFiProperties.countProtectedProperties(mergedProperties)} protected keys")

        mergedProperties
    }

    /**
     * Reads the existing {@code bootstrap.conf} file, updates it to contain the master key, and persists it back to the same location.
     *
     * @throw IOException if there is a problem reading or writing the bootstrap.conf file
     */
    private void writeKeyToBootstrapConf() throws IOException {
        File bootstrapConfFile
        if (bootstrapConfPath && (bootstrapConfFile = new File(bootstrapConfPath)).exists() && bootstrapConfFile.canRead() && bootstrapConfFile.canWrite()) {
            try {
                List<String> lines = bootstrapConfFile.readLines()

                updateBootstrapContentsWithKey(lines)

                // Write the updated values back to the file
                bootstrapConfFile.text = lines.join("\n")
            } catch (IOException e) {
                def msg = "Encountered an exception updating the bootstrap.conf file with the master key"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The bootstrap.conf file at ${bootstrapConfPath} must exist and be readable and writable by the user running this tool")
        }
    }

    /**
     * Accepts the lines of the {@code bootstrap.conf} file as a {@code List <String>} and updates or adds the key property (and associated comment).
     *
     * @param lines the lines of the bootstrap file
     * @return the updated lines
     */
    private List<String> updateBootstrapContentsWithKey(List<String> lines) {
        String keyLine = "${BOOTSTRAP_KEY_PREFIX}${keyHex}"
        // Try to locate the key property line
        int keyLineIndex = lines.findIndexOf { it.startsWith(BOOTSTRAP_KEY_PREFIX) }

        // If it was found, update inline
        if (keyLineIndex != -1) {
            logger.debug("The key property was detected in bootstrap.conf")
            lines[keyLineIndex] = keyLine
            logger.debug("The bootstrap key value was updated")

            // Ensure the comment explaining the property immediately precedes it (check for edge case where key is first line)
            int keyCommentLineIndex = keyLineIndex > 0 ? keyLineIndex - 1 : 0
            if (lines[keyCommentLineIndex] != BOOTSTRAP_KEY_COMMENT) {
                lines.add(keyCommentLineIndex, BOOTSTRAP_KEY_COMMENT)
                logger.debug("A comment explaining the bootstrap key property was added")
            }
        } else {
            // If it wasn't present originally, add the comment and key property
            lines.addAll(["\n", BOOTSTRAP_KEY_COMMENT, keyLine])
            logger.debug("The key property was not detected in bootstrap.conf so it was added along with a comment explaining it")
        }

        lines
    }

    /**
     * Writes the contents of the login identity providers configuration file with encrypted values to the output {@code login-identity-providers.xml} file.
     *
     * @throw IOException if there is a problem reading or writing the login-identity-providers.xml file
     */
    private void writeLoginIdentityProviders() throws IOException {
        if (!outputLoginIdentityProvidersPath) {
            throw new IllegalArgumentException("Cannot write encrypted properties to empty login-identity-providers.xml path")
        }

        File outputLoginIdentityProvidersFile = new File(outputLoginIdentityProvidersPath)

        if (isSafeToWrite(outputLoginIdentityProvidersFile)) {
            try {
                String updatedXmlContent
                File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)
                if (loginIdentityProvidersFile.exists() && loginIdentityProvidersFile.canRead()) {
                    // Instead of just writing the XML content to a file, this method attempts to maintain the structure of the original file and preserves comments
                    updatedXmlContent = serializeLoginIdentityProvidersAndPreserveFormat(loginIdentityProviders, loginIdentityProvidersFile).join("\n")
                }

                // Write the updated values back to the file
                outputLoginIdentityProvidersFile.text = updatedXmlContent
            } catch (IOException e) {
                def msg = "Encountered an exception updating the login-identity-providers.xml file with the encrypted values"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The login-identity-providers.xml file at ${outputLoginIdentityProvidersPath} must be writable by the user running this tool")
        }
    }

    /**
     * Writes the contents of the {@link NiFiProperties} instance with encrypted values to the output {@code nifi.properties} file.
     *
     * @throw IOException if there is a problem reading or writing the nifi.properties file
     */
    private void writeNiFiProperties() throws IOException {
        if (!outputNiFiPropertiesPath) {
            throw new IllegalArgumentException("Cannot write encrypted properties to empty nifi.properties path")
        }

        File outputNiFiPropertiesFile = new File(outputNiFiPropertiesPath)

        if (isSafeToWrite(outputNiFiPropertiesFile)) {
            try {
                List<String> linesToPersist
                File niFiPropertiesFile = new File(niFiPropertiesPath)
                if (niFiPropertiesFile.exists() && niFiPropertiesFile.canRead()) {
                    // Instead of just writing the NiFiProperties instance to a properties file, this method attempts to maintain the structure of the original file and preserves comments
                    linesToPersist = serializeNiFiPropertiesAndPreserveFormat(niFiProperties, niFiPropertiesFile)
                } else {
                    linesToPersist = serializeNiFiProperties(niFiProperties)
                }

                // Write the updated values back to the file
                outputNiFiPropertiesFile.text = linesToPersist.join("\n")
            } catch (IOException e) {
                def msg = "Encountered an exception updating the nifi.properties file with the encrypted values"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The nifi.properties file at ${outputNiFiPropertiesPath} must be writable by the user running this tool")
        }
    }

    private
    static List<String> serializeNiFiPropertiesAndPreserveFormat(NiFiProperties niFiProperties, File originalPropertiesFile) {
        List<String> lines = originalPropertiesFile.readLines()

        ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(niFiProperties)
        // Only need to replace the keys that have been protected
        Map<String, String> protectedKeys = protectedNiFiProperties.getProtectedPropertyKeys()

        protectedKeys.each { String key, String protectionScheme ->
            int l = lines.findIndexOf { it.startsWith(key) }
            if (l != -1) {
                lines[l] = "${key}=${protectedNiFiProperties.getProperty(key)}"
            }
            // Get the index of the following line (or cap at max)
            int p = l + 1 > lines.size() ? lines.size() : l + 1
            String protectionLine = "${protectedNiFiProperties.getProtectionKey(key)}=${protectionScheme}"
            if (p < lines.size() && lines.get(p).startsWith("${protectedNiFiProperties.getProtectionKey(key)}=")) {
                lines.set(p, protectionLine)
            } else {
                lines.add(p, protectionLine)
            }
        }

        lines
    }

    private static List<String> serializeNiFiProperties(NiFiProperties nifiProperties) {
        OutputStream out = new ByteArrayOutputStream()
        Writer writer = new GroovyPrintWriter(out)

        // Again, waste of memory, but respecting the interface
        Properties properties = new Properties()
        nifiProperties.getPropertyKeys().each { String key ->
            properties.setProperty(key, nifiProperties.getProperty(key))
        }

        properties.store(writer, null)
        writer.flush()
        out.toString().split("\n")
    }


    private
    static List<String> serializeLoginIdentityProvidersAndPreserveFormat(String xmlContent, File originalLoginIdentityProvidersFile) {
       def parsedXml = new XmlSlurper().parseText(xmlContent)
        def provider = parsedXml.provider.find { it.identifier == "ldap-provider" }
        def serializedProvider = new XmlUtil().serialize(provider)
        // Remove XML declaration from top
        serializedProvider = serializedProvider.replaceFirst(XML_DECLARATION_REGEX, "")

        // Find the provider element of the new XML in the file contents
        String fileContents = originalLoginIdentityProvidersFile.text
        fileContents = fileContents.replaceFirst(LDAP_PROVIDER_REGEX, serializedProvider)
        fileContents.split("\n")
    }

    /**
     * Helper method which returns true if it is "safe" to write to the provided file.
     *
     * Conditions:
     *  file does not exist and the parent directory is writable
     *  -OR-
     *  file exists and is writable
     *
     * @param fileToWrite the proposed file to be written to
     * @return true if the caller can "safely" write to this file location
     */
    private static boolean isSafeToWrite(File fileToWrite) {
        fileToWrite && ((!fileToWrite.exists() && fileToWrite.absoluteFile.parentFile.canWrite()) || (fileToWrite.exists() && fileToWrite.canWrite()))
    }

    private static String determineDefaultBootstrapConfPath() {
        String niFiToolkitPath = System.getenv(NIFI_TOOLKIT_HOME) ?: ""
        "${niFiToolkitPath ? niFiToolkitPath + "/" : ""}conf/bootstrap.conf"
    }

    private static String determineDefaultNiFiPropertiesPath() {
        String niFiToolkitPath = System.getenv(NIFI_TOOLKIT_HOME) ?: ""
        "${niFiToolkitPath ? niFiToolkitPath + "/" : ""}conf/nifi.properties"
    }

    private static String determineDefaultLoginIdentityProvidersPath() {
        String niFiToolkitPath = System.getenv(NIFI_TOOLKIT_HOME) ?: ""
        "${niFiToolkitPath ? niFiToolkitPath + "/" : ""}conf/login-identity-providers.xml"
    }

    private static String deriveKeyFromPassword(String password) {
        password = password?.trim()
        if (!password || password.length() < MIN_PASSWORD_LENGTH) {
            throw new KeyException("Cannot derive key from empty/short password -- password must be at least ${MIN_PASSWORD_LENGTH} characters")
        }

        // Generate a 128 bit salt
        byte[] salt = generateScryptSalt()
        int keyLengthInBytes = getValidKeyLengths().max() / 8
        byte[] derivedKeyBytes = SCrypt.generate(password.getBytes(StandardCharsets.UTF_8), salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, keyLengthInBytes)
        Hex.encodeHexString(derivedKeyBytes).toUpperCase()
    }

    private static byte[] generateScryptSalt() {
//        byte[] salt = new byte[16]
//        new SecureRandom().nextBytes(salt)
//        salt
        /* It is not ideal to use a static salt, but the KDF operation must be deterministic
        for a given password, and storing and retrieving the salt in bootstrap.conf causes
        compatibility concerns
        */
        "NIFI_SCRYPT_SALT".getBytes(StandardCharsets.UTF_8)
    }

    /**
     * Runs main tool logic (parsing arguments, reading files, protecting properties, and writing key and properties out to destination files).
     *
     * @param args the command-line arguments
     */
    public static void main(String[] args) {
        Security.addProvider(new BouncyCastleProvider())

        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        try {
            try {
                tool.parse(args)

                tool.keyHex = tool.getKey()

                if (!tool.keyHex) {
                    tool.printUsageAndThrow("Hex key must be provided", ExitCode.INVALID_ARGS)
                }

                try {
                    // Validate the length and format
                    tool.keyHex = parseKey(tool.keyHex)
                } catch (KeyException e) {
                    if (tool.isVerbose) {
                        logger.error("Encountered an error", e)
                    }
                    tool.printUsageAndThrow(e.getMessage(), ExitCode.INVALID_ARGS)
                }

                if (tool.migration) {
                    String migrationKeyHex = tool.getMigrationKey()

                    if (!migrationKeyHex) {
                        tool.printUsageAndThrow("Original hex key must be provided for migration", ExitCode.INVALID_ARGS)
                    }

                    try {
                        // Validate the length and format
                        tool.migrationKeyHex = parseKey(migrationKeyHex)
                    } catch (KeyException e) {
                        if (tool.isVerbose) {
                            logger.error("Encountered an error", e)
                        }
                        tool.printUsageAndThrow(e.getMessage(), ExitCode.INVALID_ARGS)
                    }
                }
                String existingKeyHex = tool.migrationKeyHex ?: tool.keyHex

                if (tool.handlingNiFiProperties) {
                    try {
                        tool.niFiProperties = tool.loadNiFiProperties(existingKeyHex)
                    } catch (Exception e) {
                        tool.printUsageAndThrow("Cannot migrate key if no previous encryption occurred", ExitCode.ERROR_READING_NIFI_PROPERTIES)
                    }
                    tool.niFiProperties = tool.encryptSensitiveProperties(tool.niFiProperties)
                }

                if (tool.handlingLoginIdentityProviders) {
                    try {
                        tool.loginIdentityProviders = tool.loadLoginIdentityProviders(existingKeyHex)
                    } catch (Exception e) {
                        tool.printUsageAndThrow("Cannot migrate key if no previous encryption occurred", ExitCode.ERROR_INCORRECT_NUMBER_OF_PASSWORDS)
                    }
                    tool.loginIdentityProviders = tool.encryptLoginIdentityProviders(tool.loginIdentityProviders)
                }
            } catch (CommandLineParseException e) {
                if (e.exitCode == ExitCode.HELP) {
                    System.exit(ExitCode.HELP.ordinal())
                }
                throw e
            } catch (Exception e) {
                if (tool.isVerbose) {
                    logger.error("Encountered an error", e)
                }
                tool.printUsageAndThrow(e.message, ExitCode.ERROR_PARSING_COMMAND_LINE)
            }

            try {
                // Do this as part of a transaction?
                synchronized (this) {
                    tool.writeKeyToBootstrapConf()
                    if (tool.handlingNiFiProperties) {
                        tool.writeNiFiProperties()
                    }
                    if (tool.handlingLoginIdentityProviders) {
                        tool.writeLoginIdentityProviders()
                    }
                }
            } catch (Exception e) {
                if (tool.isVerbose) {
                    logger.error("Encountered an error", e)
                }
                tool.printUsageAndThrow("Encountered an error writing the master key to the bootstrap.conf file and the encrypted properties to nifi.properties", ExitCode.ERROR_GENERATING_CONFIG)
            }
        } catch (CommandLineParseException e) {
            System.exit(e.exitCode.ordinal())
        }

        System.exit(ExitCode.SUCCESS.ordinal())
    }
}
