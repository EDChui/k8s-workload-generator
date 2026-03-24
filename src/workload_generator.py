import re
import time
import yaml
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.rest import ApiException

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
        name = re.sub(r'[^a-z0-9.-]+', '-', name)   # replace invalid chars, including "_"
        name = re.sub(r'^[^a-z0-9]+', '', name)     # must start with alnum
        name = re.sub(r'[^a-z0-9]+$', '', name)     # must end with alnum
        name = re.sub(r'-{2,}', '-', name)          # collapse repeated dashes
        name = name[:max_length]
        name = re.sub(r'[^a-z0-9]+$', '', name)     # re-trim after truncation
        return name or "job"

    def apply_overrides(self, doc: dict, namespace: str, job_name: str, run_id: str, node_name: Optional[str]=None) -> dict:
        root_metadata = doc.setdefault("metadata", {})
        root_metadata["name"] = job_name
        root_metadata["namespace"] = namespace
        root_labels = root_metadata.setdefault("labels", {})
        root_labels["run_id"] = run_id

        template = doc.setdefault("spec", {}).setdefault("template", {})
        template_labels = template.setdefault("metadata", {}).setdefault("labels", {})
        template_labels["run_id"] = run_id

        pod_spec = template.setdefault("spec", {})
        pod_spec.setdefault("restartPolicy", "Never")
        if node_name is not None:
            pod_spec.setdefault("nodeSelector", {})["kubernetes.io/hostname"] = node_name
        return doc
    
    def launch_single_job(self, namespace: str, job_name: str, run_id: str, node_name: Optional[str]=None) -> dict:
        job_name = self.sanitize_k8s_name(job_name)
        doc = self.load_yaml(self.template_path)
        doc = self.apply_overrides(doc, namespace, job_name, run_id, node_name)

        created = self._batch_api.create_namespaced_job(namespace=namespace, body=doc)

        payload = {
            "namespace": created.metadata.namespace,
            "job_name": created.metadata.name,
            "uid": created.metadata.uid,
            "created_at": created.metadata.creation_timestamp.isoformat() if created.metadata.creation_timestamp else None,
        }
        return payload
    
    def launch_multiple_jobs(self, namespace: str, job_name: str, run_id: str, n: int, node_name: Optional[str]=None) -> List[dict]:
        results = []
        timestamp = self.now_utc_compact()
        for i in range(n):
            unique_job_name = f"{job_name}-{timestamp}-{i}"[:63].rstrip("-").lower()
            result = self.launch_single_job(namespace, unique_job_name, run_id, node_name)
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
    
    def _delete_job(self, namespace: str, job_name: str) -> None:
        body = client.V1DeleteOptions(propagation_policy="Background")
        try:
            self._batch_api.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                body=body,
            )
            logger.info(f"Deleted job {namespace}/{job_name} from Kubernetes.")
        except ApiException as exc:
            if exc.status == 404:
                logger.info(f"Job {namespace}/{job_name} was already deleted.")
                return
            raise

    def wait_for_jobs_and_cleanup(
        self,
        launched_jobs: List[dict],
        delete_after_seconds: int = 300,
        status_poll_seconds: int = 10,
    ) -> List[dict]:
        tracked_jobs = {
            (job["namespace"], job["job_name"]): dict(job) for job in launched_jobs
        }
        remaining_jobs = set(tracked_jobs.keys())

        while remaining_jobs:
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
                    else:
                        logger.error(f"Error fetching job {namespace}/{job_name}: {e}")
                        raise
                
                if job_info.get("terminal_state") is None:
                    terminal_state, completed_at = self._get_job_terminal_state(current_job)
                    if terminal_state:
                        delete_at = (completed_at or now) + timedelta(seconds=delete_after_seconds)
                        job_info["terminal_state"] = terminal_state
                        job_info["finished_at"] = completed_at.isoformat() if completed_at else None
                        job_info["to_be_deleted_at"] = delete_at.isoformat()
                        logger.info(
                            f"Job {namespace}/{job_name} reached terminal state: {terminal_state}."
                            f" It will be deleted at {delete_at.isoformat()}."
                        )
                
                to_delete_at_str = job_info.get("to_be_deleted_at")
                if to_delete_at_str:
                    delete_at = datetime.fromisoformat(to_delete_at_str)
                    if now >= delete_at:
                        self._delete_job(namespace, job_name)
                        job_info.setdefault("terminal_state", "deleted")
                        job_info["deleted_at"] = now.isoformat()
                        remaining_jobs.remove((namespace, job_name))
            
            if remaining_jobs:
                logger.info(f"Still tracking {len(remaining_jobs)} job(s). Polling again in {status_poll_seconds} seconds...")
                time.sleep(status_poll_seconds)
        return list(tracked_jobs.values())

    def run(self, namespace: str, job_name: str, run_id: str, total_tasks: int, batch_size: int, wait_seconds: int) -> List[dict]:
        launched_jobs = []
        tasks_launched = 0
        while tasks_launched < total_tasks:
            current_batch_size = min(batch_size, total_tasks - tasks_launched)
            batch_jobs = self.launch_multiple_jobs(namespace, job_name, run_id, current_batch_size)
            launched_jobs.extend(batch_jobs)
            tasks_launched += current_batch_size

            if tasks_launched < total_tasks:
                logger.info(f"Launched {tasks_launched}/{total_tasks} tasks. Waiting for {wait_seconds} seconds before next batch...")
                time.sleep(wait_seconds)
        logger.info(f"All {total_tasks} tasks launched.")
        return launched_jobs
