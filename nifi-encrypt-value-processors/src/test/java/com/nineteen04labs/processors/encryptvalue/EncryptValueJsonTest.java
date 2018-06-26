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
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class EncryptValueJsonTest {

    private final Path unencryptedFile = Paths.get("src/test/resources/unencrypted.json");
    private final TestRunner runner = TestRunners.newTestRunner(new EncryptValue());

    @Test
    public void testSHA512() throws IOException {
        Path sha512File = Paths.get("src/test/resources/sha512.json");
        testEncryption("SHA-512", sha512File);
    }

    @Test
    public void testNoEncryption() throws IOException {
        runner.setProperty(EncryptValueProperties.FLOW_FORMAT, "JSON");
        runner.setProperty(EncryptValueProperties.HASH_ALG, "SHA-512");

        runner.enqueue(unencryptedFile);

        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(EncryptValueRelationships.REL_SUCCESS, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(EncryptValueRelationships.REL_SUCCESS).get(0);

        outFile.assertContentEquals(unencryptedFile);
    }

    private void testEncryption(final String hashAlgorithm, final Path encryptedFile) throws IOException {
        runner.setProperty(EncryptValueProperties.FIELD_NAMES, "card_number,last_name");
        runner.setProperty(EncryptValueProperties.FLOW_FORMAT, "JSON");
        runner.setProperty(EncryptValueProperties.HASH_ALG, hashAlgorithm);

        runner.enqueue(unencryptedFile);

        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(EncryptValueRelationships.REL_SUCCESS, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(EncryptValueRelationships.REL_SUCCESS).get(0);

        outFile.assertContentEquals(encryptedFile);
    }

}
