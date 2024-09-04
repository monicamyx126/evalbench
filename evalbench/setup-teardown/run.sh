#!/bin/bash
if [[ -z "${EVAL_DB_PASSWORD}" ]];
  then echo "EVAL_DB_PASSWORD is required";
  exit 0;
fi

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

python3 setup-teardown.py --setup_config_file="configs/setup_teardown.yaml"