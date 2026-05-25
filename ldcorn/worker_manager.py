import asyncio
import os
import shutil
import signal
import sys
import tempfile
import time
import uuid
from typing import Dict, List

from .config import LdConfig
from .log import logger

class WorkerManager:
    def __init__(self, config: LdConfig):
        self.config = config
        self.worker_info: List[Dict] = []
        self.temp_dir = tempfile.mkdtemp(prefix="ldcorn_")
        self.group_sockets: Dict[str, List[str]] = {}
        self.running = False
        self.monitor_task = None

    async def _spawn_worker(self, group_name: str, app: str, sock_path: str, log_level: str = "info"):
        cmd = [sys.executable, "-m", "uvicorn", app, "--uds", sock_path, "--log-level", log_level]
        logger.info(f"Spawning worker {group_name} for app {app} on {sock_path} with log level '{log_level}'")
        return await asyncio.create_subprocess_exec(*cmd)

    async def _reap_process(self, proc, sock_path=None):
        try:
            await proc.wait()
        except Exception:
            pass
        if sock_path and os.path.exists(sock_path):
            try:
                os.remove(sock_path)
            except OSError:
                pass

    def print_worker_summary(self):
        logger.info("Active worker topology:")
        for group_name, sockets in self.group_sockets.items():
            logger.info(f"  - [{group_name}]: {len(sockets)} worker(s)")

    async def start(self):
        self.running = True
        logger.info(f"Starting ldcorn workers, sockets in {self.temp_dir}")
        for group in self.config.workers:
            self.group_sockets[group.name] = []
            for i in range(group.instances):
                sock_path = os.path.join(self.temp_dir, f"{group.name}_{i}.sock")
                self.group_sockets[group.name].append(sock_path)
                
                log_level = getattr(group, "uvicorn_log_level", "info")
                proc = await self._spawn_worker(group.name, group.app, sock_path, log_level)
                self.worker_info.append({
                    "group_name": group.name,
                    "app": group.app,
                    "sock_path": sock_path,
                    "proc": proc,
                    "restart_count": 0,
                    "last_start_time": time.time()
                })
        
        logger.info("Waiting for workers to initialize and bind to sockets...")
        timeout = 60
        start_time = time.time()
        for info in self.worker_info:
            sock = info["sock_path"]
            proc = info["proc"]
            while not os.path.exists(sock):
                # Fail fast if the worker process has already crashed/exited
                if proc is not None and proc.returncode is not None:
                    raise RuntimeError(
                        f"Worker process for group '{info['group_name']}' exited prematurely with code {proc.returncode}"
                    )
                if time.time() - start_time > timeout:
                    raise TimeoutError(
                        f"Worker for group '{info['group_name']}' failed to bind to {sock} within {timeout}s!"
                    )
                await asyncio.sleep(0.1)
                    
        await asyncio.sleep(0.5)
        self.print_worker_summary()
        
        self.monitor_task = asyncio.create_task(self._monitor_workers())

    async def _monitor_workers(self):
        while self.running:
            try:
                for info in list(self.worker_info):
                    proc = info.get("proc")
                    
                    if proc is None:
                        # Waiting to respawn
                        if time.time() >= info.get("respawn_after", 0):
                            group = next((g for g in self.config.workers if g.name == info["group_name"]), None)
                            if not group: continue
                            
                            log_level = getattr(group, "uvicorn_log_level", "info")
                            try:
                                new_proc = await self._spawn_worker(info["group_name"], info["app"], info["sock_path"], log_level)
                                
                                # Concurrency Guard Check:
                                # If the manager stopped or a reload discarded this worker while we were yielding on _spawn_worker,
                                # terminate the newly spawned process immediately so it does not leak as an orphan.
                                if not self.running or info not in self.worker_info:
                                    logger.warning(f"Monitor guard triggered: newly spawned worker {info['group_name']} is orphaned. Terminating...")
                                    if new_proc.returncode is None:
                                        try:
                                            new_proc.send_signal(signal.SIGTERM)
                                        except ProcessLookupError:
                                            pass
                                    asyncio.create_task(self._reap_process(new_proc, info["sock_path"]))
                                    continue

                                info["proc"] = new_proc
                                info["last_start_time"] = time.time()
                            except Exception as e:
                                logger.error(f"Failed to spawn new worker process for group {info['group_name']}: {e}")
                                # Retry later
                                info["respawn_after"] = time.time() + 1.0
                        continue

                    if proc.returncode is not None:
                        # Process died unexpectedly
                        group = next((g for g in self.config.workers if g.name == info["group_name"]), None)
                        max_restarts = getattr(group, "max_restarts_on_crash", 3) if group else 3
                        restart_backoff = getattr(group, "restart_backoff_on_crash", 2.0) if group else 2.0
                        
                        # Reset counter if worker lived for a while (e.g. 60s)
                        if time.time() - info.get("last_start_time", time.time()) > 60.0:
                            info["restart_count"] = 0
                            
                        if info.get("restart_count", 0) >= max_restarts:
                            logger.error(f"Worker {info['group_name']} has crashed {info.get('restart_count', 0)} times. Reached max_restarts. Giving up on this instance.")
                            if os.path.exists(info["sock_path"]):
                                try:
                                    os.remove(info["sock_path"])
                                except OSError:
                                    pass
                            info["proc"] = None
                            info["respawn_after"] = float('inf')  # Never respawn
                            continue
                            
                        backoff = restart_backoff * (2 ** info.get("restart_count", 0))
                        logger.warning(f"Worker {info['group_name']} (PID {proc.pid}) died with exit code {proc.returncode}. Respawning after {backoff}s backoff (Attempt {info.get('restart_count', 0) + 1}/{max_restarts})...")
                        
                        # Unlink old socket synchronously to prevent Address already in use during respawn
                        if os.path.exists(info["sock_path"]):
                            try:
                                os.remove(info["sock_path"])
                            except OSError:
                                pass
                                
                        # Reap the dead process to prevent it from leaking as a zombie
                        asyncio.create_task(self._reap_process(proc))
                        
                        info["proc"] = None
                        info["respawn_after"] = time.time() + backoff
                        info["restart_count"] = info.get("restart_count", 0) + 1
            except Exception as e:
                logger.error(f"Unexpected error in monitor loop: {e}")
            
            # Responsive sleep check to allow rapid stopping/reloading
            for _ in range(20):
                if not self.running:
                    break
                await asyncio.sleep(0.1)

    async def stop(self):
        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            
        logger.info("Shutting down workers...")
        for info in self.worker_info:
            proc = info.get("proc")
            if proc and proc.returncode is None:
                try:
                    proc.send_signal(signal.SIGTERM)
                except ProcessLookupError:
                    pass
        
        # Reap all processes
        reap_tasks = []
        for info in self.worker_info:
            proc = info.get("proc")
            if proc:
                reap_tasks.append(proc.wait())
            
        if reap_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*reap_tasks, return_exceptions=True), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("Some workers did not shut down gracefully within 10s.")
            
        logger.info("Workers terminated.")
        
        logger.info(f"Cleaning up master socket directory: {self.temp_dir}")
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def reload(self, new_config: LdConfig) -> Dict[str, List[str]]:
        logger.info("SIGHUP received, performing graceful zero-downtime reload...")
        old_worker_info = self.worker_info
        old_group_sockets = self.group_sockets
        
        # Local staging structures to ensure reload is transactional and atomic
        staging_worker_info = []
        staging_group_sockets = {}
        preserved_info_ids = set()
        
        new_group_names = {g.name for g in new_config.workers}
        removed_groups = [name for name in old_group_sockets if name not in new_group_names]
        for name in removed_groups:
            logger.info(f"Worker group '{name}' was removed from config. Terminating its {len(old_group_sockets[name])} worker(s)...")
        
        for group in new_config.workers:
            # If user explicitly opted out of reload AND this group previously existed
            if not group.reload_on_sighup and group.name in old_group_sockets:
                old_sockets = old_group_sockets[group.name]
                old_infos = [info for info in old_worker_info if info["group_name"] == group.name]
                old_count = len(old_sockets)
                new_count = group.instances
                
                if new_count > old_count:
                    logger.info(f"Scaling up worker group '{group.name}' from {old_count} to {new_count} instances (spawning {new_count - old_count} new worker(s))...")
                elif new_count < old_count:
                    logger.info(f"Scaling down worker group '{group.name}' from {old_count} to {new_count} instances (terminating {old_count - new_count} worker(s) gracefully)...")
                else:
                    logger.info(f"Preserving existing {old_count} instances for worker group '{group.name}'.")
                
                staging_group_sockets[group.name] = []
                
                for i in range(group.instances):
                    if i < len(old_sockets):
                        old_info = old_infos[i]
                        old_proc = old_info.get("proc")
                        is_crashed = old_proc is None or old_proc.returncode is not None
                        
                        if not is_crashed:
                            # Preserve healthy existing instance
                            staging_group_sockets[group.name].append(old_sockets[i])
                            staging_worker_info.append(old_info)
                            preserved_info_ids.add(id(old_info))
                        else:
                            # Recover crashed/dead instance: spawn new process and reset crash count
                            logger.info(f"Worker {group.name} instance {i} was crashed/dead. Spawning a new one to recover on reload.")
                            
                            # Prevent the background monitor loop from trying to respawn this concurrently!
                            old_info["respawn_after"] = float('inf')
                            
                            sock_path = old_sockets[i]
                            if os.path.exists(sock_path):
                                try:
                                    os.remove(sock_path)
                                except OSError:
                                    pass
                            if old_proc:
                                asyncio.create_task(self._reap_process(old_proc))
                            
                            log_level = getattr(group, "uvicorn_log_level", "info")
                            proc = await self._spawn_worker(group.name, group.app, sock_path, log_level)
                            staging_group_sockets[group.name].append(sock_path)
                            staging_worker_info.append({
                                "group_name": group.name,
                                "app": group.app,
                                "sock_path": sock_path,
                                "proc": proc,
                                "restart_count": 0,
                                "last_start_time": time.time()
                            })
                    else:
                        # Scale up: spawn new instance
                        sock_path = os.path.join(self.temp_dir, f"{group.name}_{uuid.uuid4().hex[:8]}.sock")
                        staging_group_sockets[group.name].append(sock_path)
                        
                        log_level = getattr(group, "uvicorn_log_level", "info")
                        proc = await self._spawn_worker(group.name, group.app, sock_path, log_level)
                        staging_worker_info.append({
                            "group_name": group.name,
                            "app": group.app,
                            "sock_path": sock_path,
                            "proc": proc,
                            "restart_count": 0,
                            "last_start_time": time.time()
                        })
            else:
                if group.name in old_group_sockets:
                    old_count = len(old_group_sockets[group.name])
                    logger.info(f"Reloading worker group '{group.name}' (spawning {group.instances} new worker(s) to replace {old_count} old worker(s))...")
                else:
                    logger.info(f"Spawning new worker group '{group.name}' with {group.instances} instances...")
                
                staging_group_sockets[group.name] = []
                for i in range(group.instances):
                    sock_path = os.path.join(self.temp_dir, f"{group.name}_{uuid.uuid4().hex[:8]}.sock")
                    staging_group_sockets[group.name].append(sock_path)
                    
                    log_level = getattr(group, "uvicorn_log_level", "info")
                    proc = await self._spawn_worker(group.name, group.app, sock_path, log_level)
                    staging_worker_info.append({
                        "group_name": group.name,
                        "app": group.app,
                        "sock_path": sock_path,
                        "proc": proc,
                        "restart_count": 0,
                        "last_start_time": time.time()
                    })
                
        logger.info("Waiting for new workers to initialize and bind to sockets...")
        timeout = 60
        start_time = time.time()
        try:
            for info in staging_worker_info:
                if id(info) not in preserved_info_ids:
                    sock = info["sock_path"]
                    proc = info["proc"]
                    while not os.path.exists(sock):
                        # Fail fast if a new staging process has exited prematurely
                        if proc is not None and proc.returncode is not None:
                            raise RuntimeError(
                                f"New worker process for group '{info['group_name']}' exited prematurely with code {proc.returncode}"
                            )
                        if time.time() - start_time > timeout:
                            raise TimeoutError(
                                f"New worker for group '{info['group_name']}' failed to bind to {sock} within {timeout}s!"
                            )
                        await asyncio.sleep(0.1)
        except Exception as e:
            # Transaction rollback: Clean up newly spawned staging workers immediately
            logger.error(f"ERROR during reload: {e}. Aborting reload and keeping previous workers running.")
            for info in staging_worker_info:
                if id(info) not in preserved_info_ids:
                    proc = info.get("proc")
                    if proc and proc.returncode is None:
                        try:
                            proc.send_signal(signal.SIGTERM)
                        except ProcessLookupError:
                            pass
            
            # Reap aborted staging processes
            for info in staging_worker_info:
                if id(info) not in preserved_info_ids:
                    proc = info.get("proc")
                    if proc:
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=5.0)
                        except Exception:
                            pass
                    if os.path.exists(info["sock_path"]):
                        try:
                            os.remove(info["sock_path"])
                        except OSError:
                            pass
            
            # Propagate exception to avoid committing invalid configuration state
            raise e
                    
        # Extra small buffer to ensure Uvicorn is fully accepting connections on the socket
        await asyncio.sleep(0.5)
        
        # Transaction commit: Swap staging structures to running state
        self.config = new_config
        self.worker_info = staging_worker_info
        self.group_sockets = staging_group_sockets
        
        # Gracefully shut down old workers
        logger.info("New workers are online! Sending SIGTERM to old workers to gracefully finish active requests...")
        for info in old_worker_info:
            if id(info) not in preserved_info_ids:
                proc = info.get("proc")
                if proc and proc.returncode is None:
                    try:
                        proc.send_signal(signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                if proc:
                    # Spawn a background task to reap the old process cleanly and delete its socket
                    asyncio.create_task(self._reap_process(proc, info["sock_path"]))
                else:
                    if os.path.exists(info["sock_path"]):
                        try:
                            os.remove(info["sock_path"])
                        except OSError:
                            pass
                            
        self.print_worker_summary()
        return self.group_sockets
