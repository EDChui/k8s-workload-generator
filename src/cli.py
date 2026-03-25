import click
from pathlib import Path

from workload_generator import WorkloadGenerator


@click.command(help="Launch multiple Kubernetes jobs in batches")
@click.option("--template", required=True, help="Path to Job YAML template")
@click.option("--namespace", default="default", required=True, help="Kubernetes namespace")
@click.option("--job-name", default="demo", help="Base name for the jobs")
@click.option("--run-id", default="0", help="Unique identifier for the run")
@click.option("--total-jobs", default=20, required=True, type=int, help="Total number of jobs to launch")
@click.option("--batch-size", default=5, required=True, type=int, help="Number of jobs to launch in each batch")
@click.option("--wait-seconds", default=300, required=True, type=int, help="Seconds to wait between batches")
@click.option("--delete-after-seconds", default=300, required=True, type=click.IntRange(min=0), help="Seconds to keep a completed Job before deleting it")
@click.option("--status-poll-seconds", default=10, required=True, type=click.IntRange(min=1), help="Seconds between Kubernetes status polls while waiting for completion and cleanup")
def launch_workload_generator(
    template: str,
    namespace: str,
    job_name: str,
    run_id: str,
    total_jobs: int,
    batch_size: int,
    wait_seconds: int,
    delete_after_seconds: int,
    status_poll_seconds: int,
) -> int:
    generator = WorkloadGenerator(Path(template))
    generator.run(
        namespace=namespace,
        job_name=job_name,
        run_id=run_id,
        total_jobs=total_jobs,
        batch_size=batch_size,
        wait_seconds=wait_seconds,
        delete_after_seconds=delete_after_seconds,
        status_poll_seconds=status_poll_seconds,
    )
    return 0

if __name__ == "__main__":
    launch_workload_generator()
