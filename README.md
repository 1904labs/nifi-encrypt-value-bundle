# nifi-encrypt-value-bundle

NiFi processor to encrypt JSON values

## Deploy Bundle

Clone this repository

```
git clone https://github.com/1904labs/nifi-encrypt-value-bundle
```

Build the bundle

```
cd nifi-encrypt-value-bundle
mvn clean install
```

Copy Nar file to $NIFI_HOME/lib

```
cp nifi-encrypt-value-bundle/target/nifi-encrypt-value-nar-$version.nar $NIFI_HOME/lib/
```

Start/Restart Nifi

```
$NIFI_HOME/bin/nifi.sh start
```


### TODO

- Add support for Avro files
- Add support for more hashing algorithms
