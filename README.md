# Embulk CSV parser plugin using JDBC to define schema

This Embulk plugin extends CSV parser to define columns based on database meta data.

## Overview

* **Plugin type**: parser

## Configuration

- **schema** database table to define columns. Same as embulk-input-plugin.

Others are same as CSV parser plugin except columns (columns are ignored).

### Example

```yaml
in:
  type: file
  path_prefix: 'data/test.csv'
  parser:
    type: jdbc-schema-csv
    delimiter: ','
    header_line: false
    schema: &OUT
      host: localhost
      user: myuser
      password: ""
      database: my_database
      table: my_table
      mode: insert
out: *OUT
```

### Build

```
$ ./gradle gem
```
