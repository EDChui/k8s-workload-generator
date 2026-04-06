# K8S Workload Generator

# Softwares

- Python 3.8.10

# Sample Usage

```bash
python3 src/cli.py batch \
    --template assets/fibonacci-template.yaml \
    --namespace exp --job-name demo \
    --total-jobs 20 --batch-size 5 --wait-seconds 30 \
    --delete-after-seconds 30 --status-poll-seconds 30
```

```bash
python3 src/cli.py poisson \
    --template assets/fibonacci-template.yaml \
    --namespace exp --job-name demo \
    --total-jobs 20 --iat-seconds 30 \
    --delete-after-seconds 30 --status-poll-seconds 30 \
    --seed 42
```
