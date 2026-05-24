# Ldcorn

A master proxy and process manager for path-based routing across Uvicorn worker pools. 

## What it does
Ldcorn allows you to route incoming HTTP requests to specific Uvicorn worker processes based on the URL path. This gives you request-level load balancing across the Python GIL without needing separate deployments. For example, you can have one dedicated worker handling fast I/O queries, and another strictly reserved for heavy machine learning compute or long-lived WebSockets—all from the exact same monolithic codebase.

## Is this for you?
The majority of the time, the answer is **NO**. If you have this sort of requirement, the standard answer is almost always to split your app into microservices and use Nginx to route traffic between them. 

However, sometimes i get annoyed managing multiple deployments, dealing with code duplication, and handling schema drift is incredibly painful. That's where Ldcorn comes in. I built this because I prefer keeping my fast I/O workers completely insulated from my CPU-bound workers without managing a distributed system. You can even configure your heavy dependencies (like ML models) to lazy-load specifically on the compute workers. It gives you massive fine-grained control over a monolithic Python app.

*Fair warning:* Avoid using this as a hack for state management. You should generally rely on Redis for that. However, if you really need to pin stateful connections (like WebSockets or in-memory counters) to a single dedicated worker, Ldcorn's path routing will absolutely let you do that. Check out the included examples for proof.

### Features
- **Path-Based Routing:** Route specific endpoints (like `/ws` or `/heavy-compute`) to dedicated worker processes. *(Note: The upcoming `ldcorn-go` rewrite will also support cookie/header-based routing, but this pure-Python version intentionally opts out of those to avoid proxy overhead.)*
- **Max Requests Per Worker:** Built-in concurrency queuing. Limit specific workers to exactly `X` concurrent requests to avoid locking databases or overloading threads.
- **Zero-Downtime Hot Reloads:** Send `SIGHUP` to Ldcorn and it will elegantly spin up new workers, hot-swap the routing tables, and let the old workers gracefully finish their active requests. Zero dropped connections.
  - **Dynamic Scaling & Proxy Updates:** If you opt a worker out of SIGHUP reloads (`reload_on_sighup=False`), you can still edit your config to change its `routes` or `max_req_per_worker` and Ldcorn will instantly apply them at the proxy level without restarting the physical process!
  - **Scale Up/Down Seamlessly:** If you change an opted-out worker's `instances` from 1 to 4, Ldcorn will preserve the 1 running instance and spawn 3 new ones.
- **Auto-Restarts:** Built-in process monitoring revives crashed workers instantly.

## Quickstart

**1. Create `ldconfig.py`**
```python
from ldcorn.config import LdConfig, WorkerGroup

config = LdConfig(
    bind="127.0.0.1:8000", # for master process to listen on.
    workers=[
        WorkerGroup(
            name="default",
            app="main:app", 
            instances=2,
            max_req_per_worker=0, # Built-in concurrency queueing! 0 = unlimited
            routes=["*"], # all requests except another workergroup ones
            reload_on_sighup=True
        ),
        WorkerGroup(
            name="ml_heavy",
            app="main:app", # this does not need to be same app
            instances=1,
            max_req_per_worker=100,
            routes=["/ml-pipeline"],
            reload_on_sighup=False # Opt-out of hot-reloads so your ML model doesn't constantly reboot!
        )
    ]
)
```

**2. Start Ldcorn**
```bash
ldcorn -c ldconfig.py
```

**3. Zero-Downtime Hot Swap**
Updated your code or your config? Reload seamlessly:
```bash
kill -HUP <ldcorn_pid>
```
*Note: Changes to the `bind` parameter in your config will NOT take effect during a `SIGHUP` reload. That requires a full restart.*

If you are deploying with `systemd`, configure `ExecReload` to send `SIGHUP` to the master process. This allows you to use `systemctl reload ldcorn` for completely seamless, zero-downtime deployments. 

### ⚠️ Production Memory warning
During SIGHUP Ldcorn first creates new workers and once they are healthy then it sends graceful shutdown to old workers. so until all active requests in the old pool are completed , **both the old and the new worker pools will be running at same time**  

As a result, your application will experience a transient **2x memory usage spike** during the reload handover phase. If your system is running close to the memory limit (especially when loading large ML models or heavy frameworks), ensure you have enough swap space or free memory headroom to accommodate both pools running simultaneously to prevent the Linux Out-Of-Memory (OOM) killer from shutting down your application processes. 