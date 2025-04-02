# How to run Evalbench for [BIRD Dataset](https://bird-bench.github.io/)

To run EvalBench on the BIRD Dataset, follow these steps:

## 1. Download the Dataset
Before you can run EvalBench for BIRD, you need to download the dataset and place the necessary files in the correct directories. Specifically:

- The `.sqlite` files (databases) should be placed under `db_connections/bird/`.
- The list of evaluation items should be placed in `datasets/bird/prompts.json`.

This step is handled automatically by the script `download_dataset.sh`.

## 2. Run the Download Script
To download the dataset, execute the following command:

```bash
./datasets/bird/download_dataset.sh
```

This script will download the development dataset. If you prefer, you can also work with the [training dataset](https://bird-bench.oss-cn-beijing.aliyuncs.com/train.zip) by downloading it separately.

## 3. Set Your Evaluation Configuration
To start the evaluation, you need to configure your environment. Before executing this, make sure to review the [README](https://github.com/GoogleCloudPlatform/evalbench/blob/main/README.md) and other [config related documentation](https://github.com/GoogleCloudPlatform/evalbench/tree/main/docs/configs) to add other required export variables.

Set the EVAL_CONFIG environment variable to specify your evaluation configuration file. For example:
```bash
export EVAL_CONFIG=datasets/bird/example_run_config.yaml
```

## 4. Run EvalBench
Now, you can start the evaluation process by running the following shell script:
```bash
./evalbench/run.sh
```
