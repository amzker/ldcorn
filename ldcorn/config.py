from dataclasses import dataclass, field
from typing import List
import re

@dataclass
class WorkerGroup:
    """Configuration for a group of workers.
    
    Args:
        name: Unique name for this worker group (used for socket naming)
        app: The ASGI application import string (e.g. 'main:app')
        instances: Number of Uvicorn worker processes to spawn for this group
        max_req_per_worker: Max concurrent requests allowed in the proxy before queueing. 0 = unlimited.
        routes: List of URL prefixes to route to this group (e.g. ['/api', '/ws'])
        reload_on_sighup: Whether to restart this worker group during SIGHUP. Set to False for heavy ML models or stateful workers.
        uvicorn_log_level: Uvicorn log level (e.g. 'info', 'warning', 'error', 'critical', etc.)
        max_restarts_on_crash: Maximum number of times to restart a crashed worker before giving up.
        restart_backoff_on_crash: Base backoff time in seconds for exponential backoff on crash.
    """
    name: str
    app: str
    instances: int = 1
    max_req_per_worker: int = 0
    routes: List[str] = field(default_factory=lambda: ["*"])
    reload_on_sighup: bool = True
    uvicorn_log_level: str = "info"
    max_restarts_on_crash: int = 3
    restart_backoff_on_crash: float = 2.0

@dataclass
class LdConfig:
    """Master configuration for ldcorn.
    
    Args:
        bind: The IP and Port the Ldcorn master proxy will bind to (Note: Changing this requires a full restart, it is NOT hot-reloaded via SIGHUP)
        workers: List of worker groups to spawn and route traffic to (All worker configurations ARE hot-reloaded seamlessly on SIGHUP)
    """
    bind: str = "0.0.0.0:8000"
    workers: List[WorkerGroup] = field(default_factory=list)

def validate_config(config: LdConfig):
    if not config.workers:
        raise ValueError("Configuration error: At least one worker group must be defined in 'workers'.")
        
    seen_names = set()
    for group in config.workers:
        if not group.name:
            raise ValueError("Configuration error: Worker group 'name' cannot be empty.")
            
        if not re.match(r"^[a-zA-Z0-9_-]+$", group.name):
            raise ValueError(
                f"Configuration error: Worker group name '{group.name}' contains invalid characters. "
                "Only alphanumeric characters, dashes (-), and underscores (_) are allowed."
            )
            
        if group.name in seen_names:
            raise ValueError(f"Configuration error: Duplicate worker group name '{group.name}' found. Names must be unique.")
        seen_names.add(group.name)
        
        if not group.app or not isinstance(group.app, str):
            raise ValueError(
                f"Configuration error: Worker group '{group.name}' has invalid 'app' import path. "
                "Must be a non-empty string referencing the ASGI application (e.g., 'main:app')."
            )
            
        if not isinstance(group.instances, int) or group.instances < 1:
            raise ValueError(f"Configuration error: Worker group '{group.name}' must have 'instances' set to an integer >= 1.")
            
        if not isinstance(group.routes, list) or not group.routes:
            raise ValueError(f"Configuration error: Worker group '{group.name}' must specify a non-empty list of 'routes'.")
            
        for r in group.routes:
            if not r or not isinstance(r, str):
                raise ValueError(f"Configuration error: Route prefix in worker group '{group.name}' must be a non-empty string.")
