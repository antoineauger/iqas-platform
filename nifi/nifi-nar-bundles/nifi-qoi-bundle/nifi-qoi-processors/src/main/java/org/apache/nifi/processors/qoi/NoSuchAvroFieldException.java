package org.apache.nifi.processors.qoi;

/**
 * Created by an.auger on 04/07/2016.
 */
public class NoSuchAvroFieldException extends Exception {
    //Parameterless Constructor
    public NoSuchAvroFieldException() {}

    //Constructor that accepts a message
    public NoSuchAvroFieldException(String field)
    {
        super("No such Avro field (" + field + ").");
    }
}
