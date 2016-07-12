#!/bin/bash

cfy executions start -w uninstall -d nifi
cfy deployments delete -d nifi
cfy blueprints delete -b nifi

cfy blueprints upload -b nifi -p simple-nifi-blueprint.yaml
cfy deployments create -b nifi -d nifi --inputs inputs/cluster.yaml.template
cfy executions start -w install -d nifi