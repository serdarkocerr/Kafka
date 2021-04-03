package com.serdar.bankbalance.producer;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;

public class AvroSerializer<T> {

    public static <T>  byte[] serializeAvroMessage(SpecificRecordBase record, T clazz){
        byte result[] = null;
        try {
            Schema schema = record.getSchema();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out,null);
            DatumWriter<T> writer = new SpecificDatumWriter<>(schema);
            writer.write((T) record, encoder);
            encoder.flush();
            result = out.toByteArray();
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

}
