## Druid Custom JSON Extension

This is an example project demonstrating how to write a Druid extension. It uses the same underlying implementation for JSON `inputFormat` , but adds one extra field `ingestionTime` to the row.

### Build

To build the extension, run `mvn clean package` and you'll get a file in `target/druid-custom-json-extension-25.0.0-SNAPSHOT-bin.tar.gz`.

Unpack the tar.gz and you'll find a directory named `druid-custom-json-extension` inside it.

### Install

To install the extension:

1. Copy `druid-custom-json-extension` into your Druid `extensions` directory.
2. Edit `conf/_common/common.runtime.properties` to add `"druid-custom-json-extension"` to `druid.extensions.loadList`. (Edit `conf-quickstart/_common/common.runtime.properties` too if you are using the quickstart config.)
It should look like: `druid.extensions.loadList=["druid-example-extension"]`. There may be a few other extensions there
too.
3. Restart Druid.

### Use

Sample data -

```json
{
  "_id": "Test1-1703072029159",
  "deviceID": "Test1",
  "content": {
    "timestamp": 1703072029159,
    "temperature": 40.1
  }
}
```

The following `inputFormat` can be used for the sample data -

```json
{
  ......
  ......
  "inputFormat": {
    "type": "custom-json",
    "flattenSpec": {
      "useFieldDiscovery": true,
      "fields": [
        {
          "type": "path",
          "name": "content.timestamp",
          "expr": "$.content.timestamp",
          "nodes": null
        },
        {
          "type": "path",
          "name": "content.temperature",
          "expr": "$.content.temperature",
          "nodes": null
        }
      ]
    },
    "keepNullColumns": true,
    "assumeNewlineDelimited": true,
    "addIngestionTime": true
  }
  ......
  ......
}
```

The output of would be -

```json
{
  "_id": "Test1-1703072029159",
  "deviceID": "Test1",
  "content.timestamp": 1703072029159,
  "content.temperature": 40.1,
  "ingestionTime": 1703072049159
}
```

