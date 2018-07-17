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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatStream {

    private static final Logger logger = LoggerFactory.getLogger(FormatStream.class);
    
    public static InputStream avroToJson(InputStream in, Schema schema) throws IOException {
        GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
        DataFileStream<Object> streamReader = new DataFileStream<Object>(in, reader);
        DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);

        for (Object datum : streamReader)
            writer.write(datum, encoder);

        encoder.flush();
        baos.flush();
        streamReader.close();

        return convertStream(baos);
    }

    public static ByteArrayOutputStream jsonToAvro(ByteArrayOutputStream jsonStream, Schema schema) throws IOException {
        InputStream input = convertStream(jsonStream);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        DataFileWriter<Object> writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
        writer.setCodec(CodecFactory.snappyCodec());
        writer.create(schema, baos);

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, input);
        Object datum;

        while (true) {
            try {
                datum = reader.read(null, decoder);
            } catch (EOFException eofe) {
                break;
            }
            writer.append(datum);
        }

        writer.close();
        input.close();

        return baos;
    }

    private static InputStream convertStream(ByteArrayOutputStream baos) throws IOException {
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
