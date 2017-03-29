package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.util.List;

/**
 * Created by an.auger on 29/03/2017.
 */
@JsonldType("@list")
public class SensorCapabilityList {
    public List<SensorCapability> sensorCapabilities;
}
