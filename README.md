# tap-amplitude-snowflake

Singer tap that extracts data from Amplitude via a Snowflake database and produces JSON-formatted data following the Singer spec.

## Requirements

- pip3
- python 3.5+
- mkvirtualenv

## Installation

```
$ mkvirtualenv -p python3 tap-amplitude
$ pip3 install tap-amplitude
```

## Development

```
$ git clone git@github.com:lambtron/tap-amplitude-snowflake.git
$ cd tap-amplitude-snowflake.git
$ mkvirtualenv -p python3 tap-amplitude
$ make dev
```

## Usage

### Create config file

This config is to authenticate into your Snowflake instance.

```
{
  "username": "your_org",
  "password": "eM********",
  "account": "your_account",
  "database": "DB_88",
  "warehouse": "YOUR_WH"
}
```

### Discovery mode

This command returns a JSON that describes the schema of each table.

```
$ tap-amplitude --config config.json --discover
```

### Field selection

You can tell the tap to extract specific fields by using a `catalog.json` file.

```
$ tap-amplitude --config config.json --discover > catalog.json
```

Then, edit `catalog.json` to make selections. Note the top-level `selected` attribute, as well as the `selected` attribute nested under each property.

```
{
  "selected": "true",
  "properties": {
    "likes_getting_petted": {
      "selected": "true",
      "inclusion": "available",
      "type": [
        "null",
        "boolean"
      ]
    },
    "name": {
      "selected": "true",
      "maxLength": 255,
      "inclusion": "available",
      "type": [
        "null",
        "string"
      ]
    },
    "id": {
      "selected": "true",
      "minimum": -2147483648,
      "inclusion": "automatic",
      "maximum": 2147483647,
      "type": [
        "null",
        "integer"
      ]
    }
  },
  "type": "object"
}
```

### Sync Mode

With an annotated `catalog.json`, the tap can be invoked in sync mode:

```
$ tap-amplitude --config config.json --catalog catalog.json
```

Messages are written to standard output following the Singer specification. The resultant stream of JSON data can be consumed by a Singer target.


## Replication Methods and State File

### Incremental

Incremental replication works in conjunction with a state file to only extract new records each time the tap is invoked.


## Tests

```
$ make test
```

---

Copyright &copy; 2018 Stitch

