package fr.isae.iqas.model.entity;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

import java.util.List;

/**
 * Created by an.auger on 06/02/2017.
 */
@JsonldType("@list")
public class VirtualSensorList {
    public List<VirtualSensor> sensors;
}
