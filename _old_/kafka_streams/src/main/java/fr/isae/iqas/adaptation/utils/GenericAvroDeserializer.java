package fr.isae.iqas.adaptation.utils;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class GenericAvroDeserializer implements Deserializer<GenericRecord> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public GenericRecord deserialize(String s, byte[] bytes) {
        GenericRecord result = null;
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try {
            DataFileStream<GenericRecord> reader = new DataFileStream<>(bis, new GenericDatumReader<GenericRecord>());
            if (reader.hasNext()) {
                result = reader.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void close() {

    }
}
