package fr.isae.iqas.model.jsonld;

import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldProperty;
import ioinformarics.oss.jackson.module.jsonld.annotation.JsonldType;

/**
 * Created by an.auger on 06/02/2017.
 */
@JsonldType("http://www.w3.org/2003/01/geo/wgs84_pos#Point")
public class Location {
    @JsonldProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat")
    public String latitude;

    @JsonldProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long")
    public String longitude;

    @JsonldProperty("http://www.w3.org/2003/01/geo/wgs84_pos#alt")
    public String altitude;

    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#relativeLocation")
    public String relative_location;

    @JsonldProperty("http://purl.oclc.org/NET/UNIS/fiware/iot-lite#altRelative")
    public String relative_altitude;
}
