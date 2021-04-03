package com.serdar.bankbalance.consumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class AvroDeserilizer <T> {
    public static <T> T deserializeAvroMessage(byte[] data, Class<T> clazz, Schema schema) {
        try {
            T obj = clazz.newInstance();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data,null);
           // while (!decoder.isEnd()){
                try {
                    SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
                    reader.read(obj, decoder);
                    return obj;
                } catch (Exception e) {
                    e.printStackTrace();
                }
          //  }
        } catch (Exception e) {
            e.printStackTrace();
       }
        return null;
    }
}
