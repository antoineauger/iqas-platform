package fr.isae.iqas.adaptation.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class GenericAvroSerde<T extends GenericRecord> implements Serde<T> {

    private Schema observationSchema = null;

    public GenericAvroSerde() {
        try {
            observationSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/observation_schema.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return new Serializer<T>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public byte[] serialize(String topic, T data) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(observationSchema);
                DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
                try (DataFileWriter<GenericRecord> w = writer.create(data.getSchema(), bos)) {
                    w.append(data);
                    w.flush();
                    w.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return bos.toByteArray();
            }

            @Override
            public void close() {

            }
        };

    }

    @Override
    public Deserializer<T> deserializer() {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public T deserialize(String topic, byte[] data) {
                T result = null;
                ByteArrayInputStream bis = new ByteArrayInputStream(data);
                try {
                    DataFileStream<GenericRecord> reader = new DataFileStream<>(bis, new GenericDatumReader<GenericRecord>());
                    if (reader.hasNext()) {
                        result = (T) reader.next();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return result;
            }

            @Override
            public void close() {

            }
        };
    }
}