package fr.isae.iqas.model.observation;

import fr.isae.iqas.model.jsonld.VirtualSensor;

/**
 * Created by an.auger on 24/04/2017.
 */
public class Context {
    private String latitude;
    private String longitude;
    private String altitude;
    private String relativeLocation;
    private String topic;

    public Context() {
        this.latitude = "N/A";
        this.longitude = "N/A";
        this.altitude = "N/A";
        this.relativeLocation = "N/A";
        this.topic = "N/A";
    }

    public Context(String latitude, String longitude, String altitude, String relativeLocation, String topic) {
        this.latitude = "N/A";
        this.longitude = "N/A";
        this.altitude = "N/A";
        this.relativeLocation = "N/A";
        this.topic = "N/A";
    }

    public Context(VirtualSensor virtualSensor) {
        this.latitude = virtualSensor.location.latitude;
        this.longitude = virtualSensor.location.longitude;
        this.altitude = virtualSensor.location.altitude;
        this.relativeLocation = virtualSensor.location.relative_location;
        this.topic = virtualSensor.madeObservation.observedProperty.split("#")[1];
    }

    public String getLatitude() {
        return latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public String getAltitude() {
        return altitude;
    }

    public String getRelativeLocation() {
        return relativeLocation;
    }

    public String getTopic() {
        return topic;
    }
}
