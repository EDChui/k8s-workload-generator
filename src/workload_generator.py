import logging
import random
import re
import time
import yaml
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple, Set, Dict
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)


class WorkloadGenerator:
    def __init__(self, template_path: Path):
        config.load_kube_config()
        self._batch_api = client.BatchV1Api()
        self.template_path = template_path

    @staticmethod
    def load_yaml(path: Path) -> dict:
        with path.open() as f:
            return yaml.safe_load(f)
        
    @staticmethod
    def now_utc() -> datetime:
        return datetime.now(timezone.utc)
    
    @staticmethod
    def now_utc_compact() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    @staticmethod
    def sanitize_k8s_name(name: str, max_length: int = 63) -> str:
        name = name.lower()
        name = re.sub(r"[^a-z0-9.-]+", "-", name)   # replace invalid chars, including "_"
        name = re.sub(r"^[^a-z0-9]+", "", name)     # must start with alnum
        name = re.sub(r"[^a-z0-9]+$", "", name)     # must end with alnum
        name = re.sub(r"-{2,}", "-", name)          # collapse repeated dashes
        name = name[:max_length]
        name = re.sub(r"[^a-z0-9]+$", "", name)     # re-trim after truncation
        return name or "job"

    def apply_overrides(self, doc: dict, namespace: str, job_name: str, node_name: Optional[str]=None) -> dict:
        root_metadata = doc.setdefault("metadata", {})
        root_metadata["name"] = job_name
        root_metadata["namespace"] = namespace

        template = doc.setdefault("spec", {}).setdefault("template", {})

        pod_spec = template.setdefault("spec", {})
        pod_spec.setdefault("restartPolicy", "Never")
        if node_name is not None:
            pod_spec.setdefault("nodeSelector", {})["kubernetes.io/hostname"] = node_name
        return doc
    
    def _build_unique_job_name(self, base_job_name: str, launch_index: int, timestamp: str = None) -> str:
        if timestamp is None:
            timestamp = self.now_utc_compact()
        return f"{base_job_name}-{timestamp}-{launch_index}"[:63].rstrip("-").lower()
    
    def launch_single_job(self, namespace: str, job_name: str, node_name: Optional[str]=None) -> dict:
        job_name = self.sanitize_k8s_name(job_name)
        doc = self.load_yaml(self.template_path)
        doc = self.apply_overrides(doc, namespace, job_name, node_name)

        created = self._batch_api.create_namespaced_job(namespace=namespace, body=doc)

        payload = {
            "namespace": created.metadata.namespace,
            "job_name": created.metadata.name,
            "uid": created.metadata.uid,
            "created_at": created.metadata.creation_timestamp.isoformat() if created.metadata.creation_timestamp else None,
        }
        return payload
    
    def launch_multiple_jobs(self, namespace: str, job_name: str, n: int, node_name: Optional[str]=None) -> List[dict]:
        results = []
        timestamp = self.now_utc_compact()
        for i in range(n):
            unique_job_name = self._build_unique_job_name(job_name, i, timestamp)
            result = self.launch_single_job(namespace, unique_job_name, node_name)
            results.append(result)
        return results
    
    def _get_job_terminal_state(self, job: client.V1Job) -> Tuple[Optional[str], Optional[datetime]]:
        status = job.status
        
        for condition in status.conditions or []:
            if condition.type == "Complete" and condition.status == "True":
                return "succeeded", condition.last_transition_time
            if condition.type == "Failed" and condition.status == "True":
                return "failed", condition.last_transition_time
        return None, None
    
    def delete_job(self, namespace: str, job_name: str) -> None:
        body = client.V1DeleteOptions(propagation_policy="Background")
        try:
            self._batch_api.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                body=body,
            )
            logger.debug(f"Deleted job {namespace}/{job_name} from Kubernetes.")
        except ApiException as exc:
            if exc.status == 404:
                logger.debug(f"Job {namespace}/{job_name} was already deleted.")
                return
            raise

    def _check_and_delete_jobs(
        self,
        tracked_jobs: Dict[Tuple[str, str], dict],
        remaining_jobs: Set[Tuple[str, str]],
        delete_after_seconds: int
    ) -> None:
        """Checks the status of tracked jobs and deletes those that have been in a terminal state for longer than the specified duration."""
        now = self.now_utc()
        for namespace, job_name in list(remaining_jobs):
            job_info = tracked_jobs[(namespace, job_name)]
            try:
                current_job = self._batch_api.read_namespaced_job(name=job_name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    logger.debug(f"Job {namespace}/{job_name} not found (might have been deleted)")
                    remaining_jobs.remove((namespace, job_name))
                    job_info.setdefault("terminal_state", "deleted")
                    continue
                logger.error(f"Error fetching job {namespace}/{job_name}: {e}")
                raise
            
            # Check if the job has reached a terminal state and update tracking info if we haven't already recorded it
            if job_info.get("terminal_state") is None:
                terminal_state, completed_at = self._get_job_terminal_state(current_job)
                if terminal_state:
                    delete_at = (completed_at or now) + timedelta(seconds=delete_after_seconds)
                    job_info["terminal_state"] = terminal_state
                    job_info["finished_at"] = completed_at.isoformat() if completed_at else None
                    job_info["to_be_deleted_at"] = delete_at.isoformat()
                    logger.debug(
                        f"Job {namespace}/{job_name} reached terminal state: {terminal_state}."
                        f" It will be deleted at {delete_at.isoformat()}."
                    )
            
            # Check if it's time to delete the job
            to_delete_at_str = job_info.get("to_be_deleted_at")
            if to_delete_at_str:
                delete_at = datetime.fromisoformat(to_delete_at_str)
                if now >= delete_at:
                    self.delete_job(namespace, job_name)
                    job_info.setdefault("terminal_state", "deleted")
                    job_info["deleted_at"] = now.isoformat()
                    remaining_jobs.remove((namespace, job_name))
    
    def _wait_with_cleanup(
        self,
        tracked_jobs: Dict[Tuple[str, str], dict],
        remaining_jobs: Set[Tuple[str, str]],
        wait_seconds: int,
        delete_after_seconds: int,
        status_poll_seconds: int,
    ) -> None:
        """Waits for the specified duration while periodically checking the status of tracked jobs and deleting those that have been in a terminal state for longer than the specified duration."""
        deadline = self.now_utc() + timedelta(seconds=wait_seconds)

        while True:
            self._check_and_delete_jobs(
                tracked_jobs=tracked_jobs,
                remaining_jobs=remaining_jobs,
                delete_after_seconds=delete_after_seconds,
            )

            remaining_wait_seconds = max(0.0, (deadline - self.now_utc()).total_seconds())
            if remaining_wait_seconds <= 0:
                return

            sleep_seconds = min(status_poll_seconds, remaining_wait_seconds)
            logger.info(f"Still tracking {len(remaining_jobs)} job(s). Waiting {int(remaining_wait_seconds)} more second(s) before the next action...")
            time.sleep(sleep_seconds)

    def _track_launched_jobs(self, launched_jobs: List[dict], tracked_jobs: Dict[Tuple[str, str], dict], remaining_jobs: Set[Tuple[str, str]]) -> None:
        for job in launched_jobs:
            job_key = (job["namespace"], job["job_name"])
            tracked_jobs[job_key] = job
            remaining_jobs.add(job_key)

    def _drain_remaining_jobs(
        self,
        tracked_jobs: Dict[Tuple[str, str], dict],
        remaining_jobs: Set[Tuple[str, str]],
        total_jobs: int,
        delete_after_seconds: int,
        status_poll_seconds: int,
    ) -> List[dict]:
        logger.info(f"All {total_jobs} jobs launched. Continuing cleanup until all tracked jobs are deleted...")
        while remaining_jobs:
            self._check_and_delete_jobs(
                tracked_jobs=tracked_jobs,
                remaining_jobs=remaining_jobs,
                delete_after_seconds=delete_after_seconds,
            )
            if remaining_jobs:
                logger.info(f"Still tracking {len(remaining_jobs)} job(s). Polling again in {status_poll_seconds} seconds...")
                time.sleep(status_poll_seconds)
        return list(tracked_jobs.values())

    def run_batch(self, namespace: str, job_name: str, total_jobs: int, batch_size: int, wait_seconds: int, delete_after_seconds: int, status_poll_seconds: int) -> List[dict]:
        tracked_jobs: Dict[Tuple[str, str], dict] = {}
        remaining_jobs: Set[Tuple[str, str]] = set()
        launched_count = 0

        while launched_count < total_jobs:
            current_batch_size = min(batch_size, total_jobs - launched_count)
            batch_jobs = self.launch_multiple_jobs(namespace, job_name, current_batch_size)
            self._track_launched_jobs(batch_jobs, tracked_jobs, remaining_jobs)
            launched_count += current_batch_size

            # After launching each batch, check the status of all tracked jobs and
            # delete those that have been in a terminal state for longer than the specified duration before waiting for the next batch
            self._check_and_delete_jobs(
                tracked_jobs=tracked_jobs,
                remaining_jobs=remaining_jobs,
                delete_after_seconds=delete_after_seconds,
            )

            if launched_count < total_jobs:
                logger.info(f"Launched {launched_count}/{total_jobs} jobs. Continuing cleanup polling while waiting {wait_seconds} seconds before launching next batch...")
                self._wait_with_cleanup(
                    tracked_jobs=tracked_jobs,
                    remaining_jobs=remaining_jobs,
                    wait_seconds=wait_seconds,
                    delete_after_seconds=delete_after_seconds,
                    status_poll_seconds=status_poll_seconds,
                )
        
        return self._drain_remaining_jobs(
            tracked_jobs=tracked_jobs,
            remaining_jobs=remaining_jobs,
            total_jobs=total_jobs,
            delete_after_seconds=delete_after_seconds,
            status_poll_seconds=status_poll_seconds,
        )

    def run_poisson(self, namespace: str, job_name: str, total_jobs: int, iat_seconds: float, delete_after_seconds: int, status_poll_seconds: int, seed: int = None) -> List[dict]:
        tracked_jobs: Dict[Tuple[str, str], dict] = {}
        remaining_jobs: Set[Tuple[str, str]] = set()
        rng = random.Random(seed)

        for launched_count in range(total_jobs):
            unique_job_name = self._build_unique_job_name(job_name, launched_count)
            launched_job = self.launch_single_job(namespace, unique_job_name)
            self._track_launched_jobs([launched_job], tracked_jobs, remaining_jobs)

            self._check_and_delete_jobs(
                tracked_jobs=tracked_jobs,
                remaining_jobs=remaining_jobs,
                delete_after_seconds=delete_after_seconds,
            )

            if launched_count + 1 < total_jobs:
                sampled_wait_seconds = rng.expovariate(1.0 / iat_seconds)
                logger.info(
                    f"Launched {launched_count + 1}/{total_jobs} jobs. Waiting {sampled_wait_seconds:.2f} seconds before the next Poisson arrival (mean IAT={iat_seconds:.2f}s)..."
                )
                self._wait_with_cleanup(
                    tracked_jobs=tracked_jobs,
                    remaining_jobs=remaining_jobs,
                    wait_seconds=sampled_wait_seconds,
                    delete_after_seconds=delete_after_seconds,
                    status_poll_seconds=status_poll_seconds,
                )

        return self._drain_remaining_jobs(tracked_jobs, remaining_jobs, total_jobs, delete_after_seconds, status_poll_seconds)
