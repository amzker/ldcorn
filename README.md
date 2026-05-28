# Ldcorn

Path-based routing/grouping and process management for Uvicorn worker pools.

Ldcorn lets you split your Uvicorn workers into groups and route requests to each group by URL path. all from a single Python config file, without splitting your app into separate services.

## How it works

```text
  [ Nginx / HAProxy / ... ]
      │
      ▼
  [ Ldcorn ]
      │
      ├── /ml-pipeline  ──►  WorkerGroup "ml"        [ Worker ml1 ]
      │
      ├── /ws, /counter ──►  WorkerGroup "websocket"  [ Worker wb1 ]
      │
      └── *             ──►  WorkerGroup "default"    [ Worker d1 ]
                                                      [ Worker d2 ]
                                                      [ Worker d3 ]
```

Each worker group runs as isolated OS processes. A request routed to `ml` will never touch a `default` worker, and vice versa.


## Installation

```bash
pip install ldcorn
# or
uv add ldcorn
```

## Quickstart

### 1. Create `ldconfig.py`

```python
from ldcorn.config import LdConfig, WorkerGroup

# routes can be updated at runtime without restarting the process
config = LdConfig(
    bind="127.0.0.1:8000",
    workers=[
        WorkerGroup(
            name="default",
            app="main:app",
            instances=2,
            max_req_per_worker=0,  # 0 = unlimited
            routes=["*"],          # fallback for all unmatched routes
            reload_on_sighup=True,
            uvicorn_log_level="error",
            max_restarts_on_crash=3,
            restart_backoff_on_crash=2
        ),
        WorkerGroup(
            name="ml_heavy",
            app="main:app",        # does not need to be the same app
            instances=1,
            max_req_per_worker=100,
            routes=["/ml-pipeline"], 
            reload_on_sighup=False, # skip reloads of worker entirely.
            uvicorn_log_level="error",
            max_restarts_on_crash=3,
            restart_backoff_on_crash=2
        ),
        WorkerGroup(
            name="websocket_stateful",
            app="main:app",
            instances=1,
            max_req_per_worker=0,
            routes=["/ws", "/counter"], 
            reload_on_sighup=False,
            uvicorn_log_level="error",
            max_restarts_on_crash=3,
            restart_backoff_on_crash=2
        )
    ]
)
```

### 2. Run

```bash
ldcorn -c ldconfig.py
```

### 3. Reload on config or code change

```bash
kill -HUP <ldcorn_pid>
```

Ldcorn starts new workers, waits for them to be healthy, swaps the routing table, then lets old workers finish their active requests before exiting.

> If you use `systemd`, set `ExecReload` to send `SIGHUP` and use `systemctl reload ldcorn` for deploys.


## Is this for you?

**Use Redis or a database for shared state.** Ldcorn routes requests to worker groups. If you need shared counters, session data, or any state that survives a worker restart, use a proper store. Don't rely on in-process memory for anything that needs to be shared across machines or services. If it just needs to be consistent within a single process while your app is running, connection counters, rate tracking, WebSocket registries ,a dedicated single-instance worker group is sufficient and simpler.

**Use microservices or container orchestration** For most usecases, if you have resources and time then split into microservice. ldcorn gives you fine-grained control on a single machine. If you need to scale horizontally across machines, microservices and container orchestration is the right path.

**If your entire app is synchronous and blocking, Ldcorn won't help.** Process isolation contains the damage from blocking code. it doesn't fix it. If every endpoint blocks, every worker group will still saturate. The solution there is fixing the blocking code.


## Use cases:
Ldcorn is effective when you need fine-grained control over how a single machine's workers are allocated and routed:

**Resource allocation and priority tiering.** Route `/api/premium/*` to a dedicated group of workers with no concurrency cap, while a separate group handles free-tier traffic with stricter limits. Each group is sized independently.

**Asymmetric worker allocation.** Heavy compute like ML inference might need 6 workers to stay responsive, while your fast I/O endpoints handle load fine with 2. With round-robin you scale everything uniformly. With Ldcorn you allocate exactly where it's needed.

**Routing between different apps or codebases.** Each worker group can run a completely different ASGI application . `modern:app` alongside `legacy:app`, for example. This lets you route traffic between codebases or gradually migrate individual API paths without setting up a separate reverse proxy.

**Dynamic routing and config updates.** Reroute a path, adjust concurrency limits, or scale a worker group up or down. Ldcorn applies these changes at the routing layer without restarting worker processes. Useful for runtime adjustments without a full deploy.

## Features

**Path routing with longest prefix match.** Routes are matched by the longest matching prefix. Ties between equal-length prefixes resolve by definition order in your config.

**Per-group concurrency limits.** `max_req_per_worker` caps how many concurrent requests a single worker handles. Requests over the limit queue asynchronously and wait for a free slot, they don't spill into other groups.

**Hot config updates without process restarts.** Set `reload_on_sighup=False` on a group to exclude it from SIGHUP worker restarts. You can still update its `routes`, `max_req_per_worker`, or `instances` in the config. those changes apply at the routing layer immediately without touching the process.

**Scaling without full restart.** Increase `instances` from 1 to 4 on an opted-out group and Ldcorn keeps the existing process running and spawns the 3 additional ones.

**Auto-restart on crash.** Workers that crash are automatically restarted with configurable backoff (`restart_backoff_on_crash`).


## Benchmarks

Both scenarios use the same setup:

- **Hardware:** 16-core Intel Core i7-13620H
- **Database:** MongoDB via `motor` (async)
- **Load:** 100 concurrent connections on fast endpoints, 10 on heavy endpoints, 120s duration
- **Uvicorn:** 6 workers, round-robin
- **Ldcorn:** 6 workers total , 3 default, 1 ml, 1 math, 1 websocket

### Scenario 1: Fully async, no blocking code

All endpoints use async drivers. CPU work is offloaded with `asyncio.to_thread`.

| Endpoint | Ldcorn Group | Ldcorn (Req/s) | Uvicorn (Req/s) | Diff |
| :--- | :--- | :--- | :--- | :--- |
| Fast I/O | `default` : 3 workers | 1,438 | 1,699 | -15.3% |
| DB Read | `default` : 3 workers | 152 | 178 | -14.4% |
| DB Insert | `default` : 3 workers | 149 | 183 | -18.7% |
| DB Upsert | `default` : 3 workers | 148 | 184 | -19.6% |
| DB Delete | `default` : 3 workers | 151 | 181 | -16.5% |
| Math Prime | `math` : 1 worker | 171 | 173 | -1.3% |
| Heavy ML | `ml` : 1 worker | 56 | 88 | -36.9% |
| Stateful Counter | `websocket` : 1 worker | 201 | 197 | +1.8% |

The -15% on `default` endpoints is not Ldcorn overhead. it's because only 3 of the 6 workers are assigned to that group. The rest are occupied with their own routes. In a real mixed workload, that separation is the point. counter value stays consistent because all counter requests route to the same worker process.

When your app is fully async with no blocking code, round-robin Uvicorn is the better choice if you are aiming to improve performance via ldcorn.

### Scenario 2: Mixed workload with blocking endpoints

`time.sleep(random.uniform(1, 3))` added to the ML and Math handlers. This simulates a realistic mixed workload: some endpoints are well-optimized async, others are slow due to CPU-bound work, sync DB calls or third-party integrations that can't easily be made async.

| Endpoint | Ldcorn Group | Ldcorn (Req/s) | Uvicorn (Req/s) | Diff |
| :--- | :--- | :--- | :--- | :--- |
| Fast I/O | `default` : 3 workers | **950** | 32 | +2,899% |
| DB Read | `default` : 3 workers | **101** | 3 | +2,972% |
| DB Insert | `default` : 3 workers | **99** | 2 | +4,867% |
| DB Upsert | `default` : 3 workers | **99** | 2 | +4,843% |
| DB Delete | `default` : 3 workers | **101** | 2 | +5,069% |
| Math Prime | `math` : 1 worker | 0.52 | 1 | — |
| Heavy ML | `ml` : 1 worker | 0.50 | 1 | — |
| Stateful Counter | `websocket` : 1 worker | **133** | 4 | +3,362% |

With blocking on the ML and Math handlers, Uvicorn's round-robin distributes those requests across all 6 workers. All 6 event loops freeze. Fast I/O drops to 32 req/s.

Ldcorn's ML and Math workers saturate the same way the blocking is still there, it's just contained. The `default` and `websocket` groups continue at full throughput with no awareness of what's happening in the other groups.

The time.sleep here is just a stand-in for anything that occupies a worker, a slow ML inference call, a CPU-bound computation, a third-party API with unpredictable latency, or even just a route that legitimately handles long-lived connections. The specific cause doesn't matter. What matters is that whatever is happening in one group stays in that group. The same isolation that protects fast I/O from a blocked ML worker is what lets you give premium users a dedicated pool, pin a WebSocket to a single process, or route two different codebases from one entry point. All for the same reason: requests to one group never affect workers in another.

All benchmarks are reproducible, see `/examples`.


## Production notes

**Memory during reloads.** On SIGHUP, Ldcorn starts new workers before shutting down old ones. Both pools run simultaneously during the handover. If your workers load large ML models or heavy frameworks, make sure you have enough free memory or swap for the transient 2x spike.

**Run behind a reverse proxy.** Ldcorn does not inject `X-Forwarded-For` or `X-Real-IP` headers. Put Nginx (or HAProxy or Traefik) in front and configure it to set those headers so your workers see the correct client IPs.

**Keep-Alive.** Connections between the client and Ldcorn are closed after each request. This is intentional for hot-swap stability. For typical API traffic the overhead is negligible.

**`bind` changes need a full restart.** Changing the `bind` address does not take effect on SIGHUP , only on a full process restart.



## TODO:

- **Alternative worker runtimes.** Support for other worker types such as `gevent`.
- **Pre/Post hooks.** Extensibility hooks for worker lifecycle and routing events.
- **Visibility and metrics.** Internal metrics for monitoring request queues and worker health.
- **Queue wait timeouts / disconnect monitoring.** Dropping abandoned requests from the concurrency queue if the client disconnects before a worker becomes available.



## License

MIT [LICENSE](LICENSE).