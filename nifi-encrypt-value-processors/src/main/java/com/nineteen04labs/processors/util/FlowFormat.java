/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nineteen04labs.processors.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowFormat {

    private static final Logger logger = LoggerFactory.getLogger(FlowFormat.class);
    
    public static InputStream avroToJson(InputStream input, String schemaString) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaString);

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileStream<GenericRecord> streamReader = new DataFileStream<GenericRecord>(input, reader);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);

        for (GenericRecord datum : streamReader) {
            writer.write(datum, encoder);
        }

        streamReader.close();
        encoder.flush();
        baos.flush();

        return convertStream(baos);
    }

    public static byte[] jsonToAvro(String json, String schemaStr) throws IOException {
        InputStream input = null;
        DataFileWriter<GenericRecord> writer = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(schemaStr);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            input = new ByteArrayInputStream(json.getBytes());
            output = new ByteArrayOutputStream();
            DataInputStream din = new DataInputStream(input);
            writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
            writer.create(schema, output);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.append(datum);
            }
            writer.flush();
            writer.close();
            return output.toByteArray();
        } finally {
            try { input.close(); } catch (Exception e) { }
        }
    }

    public static InputStream convertStream(ByteArrayOutputStream baos) throws IOException {
        PipedInputStream pin = new PipedInputStream();
        PipedOutputStream pout = new PipedOutputStream(pin);

        new Thread(
            new Runnable() {
                public void run() {
                    try {
                        baos.writeTo(pout);
                        pout.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        ).start();

        return pin;
    }
}
