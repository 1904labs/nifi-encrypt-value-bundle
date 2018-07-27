# nifi-encrypt-value-bundle

NiFi processor to encrypt JSON values. Encrypts the values of the given fields of a FlowFile. The original value is replaced with the hashed one.

## Deploy Bundle

Clone this repository

```shell
git clone https://github.com/1904labs/nifi-encrypt-value-bundle
```

Build the bundle

```shell
cd nifi-encrypt-value-bundle
mvn validate
mvn clean package
```

Copy Nar file to $NIFI_HOME/lib

```shell
cp nifi-encrypt-value-bundle/target/nifi-encrypt-value-nar-$version.nar $NIFI_HOME/lib/
```

Start/Restart Nifi

```shell
$NIFI_HOME/bin/nifi.sh start
```

## Processor properties

__FlowFile Format__
Specify the format of the incoming FlowFile. If AVRO, output is automatically Snappy compressed.

__Avro Schema__
Specify the schema if the FlowFile format is Avro.

__Field Names__
Comma separated list of fields whose values to encrypt.

__Hash Algorithm__
Determines what hashing algorithm should be used to perform the encryption

### Notes

- The incoming FlowFile is expected to be one JSON per line.
- If the `Field Names` property is not set, the processor automatically sends the FlowFile to the `bypass` relationship.
- Avro is always Snappy compressed on output.
- This processor uses a [custom Avro library](https://github.com/zolyfarkas/avro) in order to handle Avro's union types. Until [this issue](https://issues.apache.org/jira/browse/AVRO-1582) is resolved, it will continue to use the custom library.


### TODO

- ~~Add support for Avro files~~
- ~~Support multi-level JSON~~
- ~~Add support for more hashing algorithms~~
- ~~Support salting~~
- Allow choice of Avro compression (Snappy, bzip2, etc.)
- ~~Infer Avro schema if not passed in~~
- Better unit tests for Avro
