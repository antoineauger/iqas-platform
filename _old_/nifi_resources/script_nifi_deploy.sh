#!/bin/bash

./bin/nifi.sh stop
cp /Users/an.auger/Documents/GIT/iQAS_platform/nifi/nifi-nar-bundles/nifi-qoi-bundle/nifi-qoi-nar/target/nifi-qoi-nar-1.0.0-SNAPSHOT.nar /Users/an.auger/Documents/GIT/iQAS_platform/nifi/nifi-assembly/target/nifi-1.0.0-SNAPSHOT-bin/nifi-1.0.0-SNAPSHOT/lib/nifi-qoi-nar-1.0.0-SNAPSHOT.nar
./bin/nifi.sh start