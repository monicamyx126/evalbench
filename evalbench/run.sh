#!/bin/bash

if [[ -z "${EVAL_DB_PASSWORD}" ]];
  then echo "EVAL_DB_PASSWORD is required";
  exit 0;
fi

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
# python3 evalbench.py   --experiment_config=configs/base_experiment_gemini.yaml
python3 evalbench.py   --experiment_config=configs/base_experiment_magick.yaml

# python3 dataset/dataset.py   --source_dataset_path=../datasets/bird_pg_dev/financial.json
# python3 dataset/dataset.py   --source_dataset_path=../datasets/hackathon/air_travel.json