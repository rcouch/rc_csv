#refuge_csv

**2012 (c) Nicolas R Dufour <nicolas.dufour@nemoworld.info>**

**refuge_csv** is a Refuge extension to enable the import and processing of CSV files in a given database.

##Requirements

* Refuge 0.4
* Erlang

## How to use it

Create a db:
    curl -XPUT http://localhost:15984/foo

Then import a CSV:
    curl -XPOST http://localhost:15984/foo/_csv --data-binary @./sample.csv

## Parameters

- delimiter: set the character used as delimiter. Defaulted to `,` (comma).
- transform: function used to transform a given row (not yet implemented)

## TODO

- create doc from row
- transform support
