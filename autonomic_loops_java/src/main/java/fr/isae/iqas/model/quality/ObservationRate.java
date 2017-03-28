package fr.isae.iqas.model.quality;

import java.util.concurrent.TimeUnit;

/**
 * Created by an.auger on 27/03/2017.
 */
public class ObservationRate {
    private long value;
    private TimeUnit unit;

    public ObservationRate(long value, TimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public long getValue() {
        return value;
    }

    public TimeUnit getUnit() {
        return unit;
    }
}
