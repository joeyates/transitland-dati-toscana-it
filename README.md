# transitland-dati-toscana-it

This repository contains the code to transform the GTFS traffic feeds
of the Tuscany region into a DMFR JSON file for the Transitland Datastore.

# Workflow

```sh
bin/regenerate.exs
```

Copy `dati.toscana.it.dmfr.json` to the transitland-atlas repository,
under `feeds`.

In the transitland-atlas repository, run:

```sh
scripts/install-transitland-lib.sh
wget https://dmfr.transit.land/json-schema/dmfr.schema-v0.5.0.json -O dmfr.schema.json
npx ajv-cli validate -s dmfr.schema.json -d "feeds/*.json"
transitland dmfr format --save feeds/dati.toscana.it.dmfr.json
transitland dmfr lint feeds/dati.toscana.it.dmfr.json
pip install pipenv
(cd scripts && pipenv install)
(cd scripts && python validate-feeds.py)
```

Commit and PR.
