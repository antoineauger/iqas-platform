package fr.isae.iqas.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by an.auger on 26/04/2017.
 */
public class Config {
    private final Properties prop;
    private final OntoConfig ontoConfig;

    public Config(Properties prop, OntoConfig ontoConfig) {
        this.prop = prop;
        this.ontoConfig = ontoConfig;
    }

    public Properties getProp() {
        return prop;
    }

    public OntoConfig getOntoConfig() {
        return ontoConfig;
    }

    public List<String> getAllPrefixes() {
        List<String> allPrefixes = new ArrayList<>();
        for (OntologyNS ns : ontoConfig.ns) {
            allPrefixes.add(ns.prefix);
        }
        return allPrefixes;
    }

    public String getIRIForPrefix(String prefix, boolean withLastChar) {
        for (OntologyNS ns : ontoConfig.ns) {
            if (ns.prefix.equals(prefix)) {
                String IRIToReturn = ns.IRI;
                if (!withLastChar) {
                    IRIToReturn = IRIToReturn.substring(0, ns.IRI.length()-1);
                }
                return IRIToReturn;
            }
        }
        return null;
    }

    public String getFilePathForPrefix(String prefix) {
        for (FileLocation file_location : ontoConfig.file_locations) {
            if (file_location.forPrefix.equals(prefix)) {
                return file_location.path;
            }
        }
        return null;
    }
}
