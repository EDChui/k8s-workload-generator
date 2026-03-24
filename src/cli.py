import click
from pathlib import Path

from workload_generator import WorkloadGenerator


@click.command(help="Launch multiple Kubernetes jobs in batches")
@click.option("--template", required=True, help="Path to Job YAML template")
@click.option("--namespace", default="default", required=True, help="Kubernetes namespace")
@click.option("--job-name", default="demo",required=True, help="Base name for the jobs")
@click.option("--run-id", default="0", help="Unique identifier for the run")
@click.option("--total-tasks", default=20, required=True, type=int, help="Total number of tasks to launch")
@click.option("--batch-size", default=5, required=True, type=int, help="Number of tasks to launch in each batch")
@click.option("--wait-seconds", default=300, required=True, type=int, help="Seconds to wait between batches")
def launch_workload_generator(
    template: str,
    namespace: str,
    job_name: str,
    run_id: str,
    total_tasks: int,
    batch_size: int,
    wait_seconds: int
) -> int:
    generator = WorkloadGenerator(Path(template))
    generator.run(namespace, job_name, run_id, total_tasks, batch_size, wait_seconds)
    return 0

if __name__ == "__main__":
    launch_workload_generator()
