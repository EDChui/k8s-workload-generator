# K8S Workload Generator

# Softwares

- Python 3.8.10

# Sample Usage

```bash
 python3 k8s-workload-generator/src/cli.py \
    --template k8s-workload-generator/assets/fibonacci-template.yaml \
    --namespace exp --job-name demo --run-id 0 \
    --total-jobs 20 --batch-size 5 --wait-seconds 30 \
    --delete-after-seconds 30 --status-poll-seconds 30
```
