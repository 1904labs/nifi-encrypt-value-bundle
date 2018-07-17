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
package com.nineteen04labs.processors.encryptvalue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class EncryptValueAvroTest {

    private final Path unencryptedFile = Paths.get("src/test/resources/unencrypted.avro");
    private String avroSchema = "";
    private final TestRunner runner = TestRunners.newTestRunner(new EncryptValue());

    @Before
    public void setSchema() throws IOException {
        avroSchema = FileUtils.readFileToString(FileUtils.getFile("src/test/resources/unencrypted.avsc"), StandardCharsets.UTF_8);
    }

    @Test
    public void testSHA512() throws IOException {
        testEncryption("SHA-512");
    }

    @Test
    public void testNoEncryption() throws IOException {
        runner.setProperty(EncryptValueProperties.FLOW_FORMAT, "AVRO");
        runner.setProperty(EncryptValueProperties.AVRO_SCHEMA, avroSchema);
        runner.setProperty(EncryptValueProperties.HASH_ALG, "SHA-512");
        runner.setValidateExpressionUsage(false);

        runner.enqueue(unencryptedFile);

        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(EncryptValueRelationships.REL_BYPASS, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(EncryptValueRelationships.REL_BYPASS).get(0);

        outFile.assertContentEquals(unencryptedFile);
    }

    private void testEncryption(final String hashAlgorithm) throws IOException {
        runner.setProperty(EncryptValueProperties.FIELD_NAMES, "first_name,last_name,card_number");
        runner.setProperty(EncryptValueProperties.FLOW_FORMAT, "AVRO");
        runner.setProperty(EncryptValueProperties.AVRO_SCHEMA, avroSchema);
        runner.setProperty(EncryptValueProperties.HASH_ALG, hashAlgorithm);
        runner.setValidateExpressionUsage(false);

        runner.enqueue(unencryptedFile);

        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(EncryptValueRelationships.REL_SUCCESS, 1);
    }

}
