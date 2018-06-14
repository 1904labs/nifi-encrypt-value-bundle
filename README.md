# nifi-encrypt-value-bundle

NiFi processor to encrypt JSON values

## Deploy Bundle

Clone this repository

```shell
git clone https://github.com/1904labs/nifi-encrypt-value-bundle
```

Build the bundle

```shell
cd nifi-encrypt-value-bundle
mvn clean install
```

Copy Nar file to $NIFI_HOME/lib

```shell
cp nifi-encrypt-value-bundle/target/nifi-encrypt-value-nar-$version.nar $NIFI_HOME/lib/
```

Start/Restart Nifi

```shell
$NIFI_HOME/bin/nifi.sh start
```


### TODO

- Add support for Avro files
- Support multi-level JSON
- ~~Add support for more hashing algorithms~~
