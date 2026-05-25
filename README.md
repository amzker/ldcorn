# Ldcorn (WIP)

A master proxy and process manager for path-based routing across Uvicorn worker pools. 

## What it does
Ldcorn allows you to route incoming HTTP requests to specific Uvicorn worker processes based on the URL path. This gives you request-level load balancing across the multiple worker processes without needing separate deployments. For example, you can have one dedicated worker handling fast I/O queries, and another strictly reserved for heavy machine learning compute or long-lived WebSockets, all from the exact same monolithic codebase.

## Is this for you?
The majority of the time, the answer is **NO**. If you have this sort of requirement, the standard answer is almost always to split your app into microservices and use Nginx to route traffic between them. 

However, sometimes i get annoyed managing multiple deployments, dealing with code duplication, and handling schema drift is incredibly painful. That's where Ldcorn comes in. I built this because I prefer keeping my fast I/O workers completely insulated from my CPU-bound workers without managing a distributed system. You can even configure your heavy dependencies (like ML models) to lazy-load specifically on the compute workers. It gives you massive fine-grained control over a monolithic Python app.

*Fair warning:* Avoid using this as a hack for state management. You should generally rely on Redis for that. However, if you really need to pin stateful connections (like WebSockets or in-memory counters) to a single dedicated worker, Ldcorn's path routing will absolutely let you do that. Check out the included examples for proof.

### Features
- **Path-Based Routing:** Route specific endpoints (like `/ws` or `/heavy-compute`) to dedicated worker processes. Ldcorn uses **Longest Prefix Match** for routing. If multiple groups share routes of the exact same length, the tie-breaker is their top-to-bottom definition order in your `ldconfig.py`. 
- **Max Requests Per Worker:** Built-in concurrency queuing. Limit specific workers to exactly `X` concurrent requests to avoid locking databases or overloading threads. Requests exceeding this limit will queue asynchronously and wait for an available slot in that specific group (they do *not* fail over to other groups).
- **Zero-Downtime Hot Reloads:** Send `SIGHUP` to Ldcorn and it will elegantly spin up new workers, hot-swap the routing tables, and let the old workers gracefully finish their active requests. Zero dropped connections.
  - **Dynamic Scaling & Proxy Updates:** If you opt a worker out of SIGHUP reloads (`reload_on_sighup=False`), you can still edit your config to change its `routes` or `max_req_per_worker` and Ldcorn will instantly apply them at the proxy level without restarting the physical process!
  - **Scale Up/Down Seamlessly:** If you change an opted-out worker's `instances` from 1 to 4, Ldcorn will preserve the 1 running instance and spawn 3 new ones.
- **Auto-Restarts:** Built-in process monitoring revives crashed workers instantly.

## Quickstart

## Install
```
uv add ldcorn
```

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
            reload_on_sighup=True,
            max_restarts_on_crash=3,
            restart_backoff_on_crash=2
        ),
        WorkerGroup(
            name="ml_heavy",
            app="main:app", # this does not need to be same app
            instances=1,
            max_req_per_worker=100,
            routes=["/ml-pipeline"],
            reload_on_sighup=False, # Opt-out of hot-reloads so your ML model doesn't constantly reboot!
            max_restarts_on_crash=3,
            restart_backoff_on_crash=2
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

### ⚠️ Production Deployment & Reverse Proxy
Ldcorn currently does not automatically inject `X-Forwarded-For` or `X-Real-IP` headers. To ensure your worker processes receive the correct client IP addresses and protocol schemes, you **must run Ldcorn behind a reverse proxy** like Nginx, HAProxy, or Traefik that is configured to set these headers. 

### ⚠️ Connection Keep-Alive
Ldcorn proxies traffic transparently but does not currently maintain persistent HTTP Keep-Alive connections between the client and the proxy itself. All connections are closed after the request is fulfilled due to hotreload. Performance overhead is negligible and can be discarded given how much performance boost you get due to request isolation per process.


## Benchmarks: The "Perfect" Async Scenario

The following benchmarks compare Ldcorn against vanilla Uvicorn in a **perfectly non-blocking** scenario. 

**Setup:**
- Load tester running 100 concurrent fast connections, and 10 concurrent connections for heavy/database endpoints. Total duration: **120 seconds**.
- Database: MongoDB via `motor` (fully async connection pooling, no event loop blocking).
- Compute: Heavy ML and Math functions fully offloaded to `asyncio.to_thread()`.
- Hardware: 16-core 13th Gen Intel(R) Core(TM) i7-13620H

- **Uvicorn:** `uvicorn app:app --workers 6 --log-level error`
- **Ldcorn:** Total of 6 workers, strictly isolated by endpoint. (3 default , 1 ML , 1 math , 1 websocket_stateful)

<table>
  <thead>
    <tr>
      <th>Endpoint</th>
      <th>Ldcorn Group (workers)</th>
      <th>Ldcorn (Req/s)</th>
      <th>Uvicorn (Req/s)</th>
      <th>Difference</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><b>Fast I/O</b></td>
      <td rowspan="5"><code>default</code> : 3 workers<br><small>handles all unmatched routes</small></td>
      <td>1,438</td>
      <td>1,699</td>
      <td>-15.3%</td>
    </tr>
    <tr>
      <td><b>DB Read</b></td>
      <td>152</td>
      <td>178</td>
      <td>-14.4%</td>
    </tr>
    <tr>
      <td><b>DB Insert</b></td>
      <td>149</td>
      <td>183</td>
      <td>-18.7%</td>
    </tr>
    <tr>
      <td><b>DB Upsert</b></td>
      <td>148</td>
      <td>184</td>
      <td>-19.6%</td>
    </tr>
    <tr>
      <td><b>DB Delete</b></td>
      <td>151</td>
      <td>181</td>
      <td>-16.5%</td>
    </tr>
    <tr>
      <td><b>Math Prime</b></td>
      <td><code>math</code> : 1 worker<br><small>dedicated to <code>/math</code></small></td>
      <td>171</td>
      <td>173</td>
      <td>-1.3%</td>
    </tr>
    <tr>
      <td><b>Heavy ML</b></td>
      <td><code>ml</code> : 1 worker<br><small>dedicated to <code>/ml-pipeline</code></small></td>
      <td>56</td>
      <td>88</td>
      <td>-36.9%</td>
    </tr>
    <tr>
      <td><b>Stateful Counter</b></td>
      <td><code>websocket_stateful</code> : 1 worker<br><small>dedicated to <code>/ws</code>, <code>/counter</code></small></td>
      <td>201</td>
      <td>197</td>
      <td>+1.8%</td>
    </tr>
    <tr>
      <td><b>TCP Edge Cases</b> *</td>
      <td>—</td>
      <td>143</td>
      <td>772</td>
      <td>-81.4%</td>
    </tr>
  </tbody>
</table>

> *\* Raw TCP gap reflects proxy hop overhead on zero-work requests with no application logic. This cost is invisible on real endpoints. fast I/O avg latency is 78ms vs 58ms at 100 concurrent.*

> **Important:** This is not ldcorn overhead. Notice that math (1 worker) nearly matches uvicorn's 6-worker throughput. The "loss" on other endpoints is purely because ldcorn intentionally assigns fewer workers to each route,those workers are free and not being utilized here. In a real mixed workload with any blocking code, those isolated workers are what keep your fast endpoints alive while the heavy ones are under load.

### Why Uvicorn Won This Round
When an application is coded perfectly using pure async non-blocking drivers (`motor`) and completely offloading CPU tasks to thread pools (`to_thread`) on powerful hardware (big BUT here) the Python asyncio event loop is never blocked. In a perfectly non-blocking environment, round-robin distribution is mathematically optimal.

### So why ldcorn?
Real-world production apps are rarely perfect in matter of full async. in such scenarios ldcorn helps , but if you have fully async production app then round robin of uvicorn, gunicorn is best for you.

**Ldcorn is an architecture firewall against messy legacy code.** Additionally, it provides zero-downtime hot reloads, dynamic proxy scaling, and state pinning out-of-the-box. which AGAIN can be done by using microservice as well as kube and sticky request by load balancer. 

- its very small places where ldcorn has its use. in most cases round robin wins effectively 
- state management part is there and process isolation is also very useful features, also knowing before hand how much concurrency you will get or what will be spike usage , fine grain control and process isolation helps a lot. you could based on load during runtime swap and hot reload , scale up and down , add more workers from python itself.


## Benchmarks: The "Messy" Real-World Scenario
while writing this doc i felt like i am underselling ldcorn so here is somewhat blocking code.

i added this in both math and ml routes 
```python
    random_time = random.uniform(1,3)
    time.sleep(random_time) # well in real prod app this is not going be there at all, if it is then you have bigger problems , checkout loopsentry to find blocks.
    # common real-world blockers: sync ORMs (SQLAlchemy without async), requests library, legacy DB drivers, CPU-bound ML inference without to_thread, subprocess calls etc... or just some random running loop 
```

i will be dammed , did not expected this much diff

Here is what happens when that blocking `time.sleep()` is introduced to the ML and Math endpoints. 

<table>
  <thead>
    <tr>
      <th>Endpoint</th>
      <th>Ldcorn Group (workers)</th>
      <th>Ldcorn Total</th>
      <th>Ldcorn (Req/s)</th>
      <th>Uvicorn Total</th>
      <th>Uvicorn 6w (Req/s)</th>
      <th>Difference</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><b>Fast I/O</b></td>
      <td rowspan="5"><code>default</code>: 3 workers<br><small>completely unaffected</small></td>
      <td><b>114,164</b></td><td><b>950</b></td><td>4,077</td><td>32</td><td>+2,899%</td>
    </tr>
    <tr>
      <td><b>DB Read</b></td>
      <td><b>12,149</b></td><td><b>101</b></td><td>423</td><td>3</td><td>+2,972%</td>
    </tr>
    <tr>
      <td><b>DB Insert</b></td>
      <td><b>11,932</b></td><td><b>99</b></td><td>214</td><td>2</td><td>+4,867%</td>
    </tr>
    <tr>
      <td><b>DB Upsert</b></td>
      <td><b>11,874</b></td><td><b>99</b></td><td>263</td><td>2</td><td>+4,843%</td>
    </tr>
    <tr>
      <td><b>DB Delete</b></td>
      <td><b>12,160</b></td><td><b>101</b></td><td>230</td><td>2</td><td>+5,069%</td>
    </tr>
    <tr>
      <td><b>Math Prime</b></td>
      <td><code>math</code> : 1 worker<br><small>blocked: blast radius contained</small></td>
      <td>71</td><td>0.52</td><td>191</td><td>1</td><td>—</td>
    </tr>
    <tr>
      <td><b>Heavy ML</b></td>
      <td><code>ml</code> : 1 worker<br><small>blocked: blast radius contained</small></td>
      <td>70</td><td>0.50</td><td>174</td><td>1</td><td>—</td>
    </tr>
    <tr>
      <td><b>Stateful Counter</b></td>
      <td><code>websocket_stateful</code> : 1 worker<br><small>completely unaffected</small></td>
      <td><b>15,964</b></td><td><b>133</b></td><td>487</td><td>4</td><td>+3,362%</td>
    </tr>
  </tbody>
</table>

<small>Blocking <code>time.sleep(1–3s)</code> added to ML + Math routes starved all 6 uvicorn workers simultaneously, collapsing every endpoint to near-zero. Ldcorn's isolated groups contained the damage, <code>default</code> and <code>websocket_stateful</code> continued serving normally, completely unaware of the blockage in <code>ml</code> and <code>math</code>.</small>


### The Catastrophic Uvicorn Failure
Look at what happened to Uvicorn: **Fast I/O dropped by 98%** (from 1,698 req/s down to 31 req/s). 
Why? Because Uvicorn routes requests round-robin. Every time a user requested the ML or Math endpoint, that Uvicorn worker's event loop completely froze for 1-3 seconds. so **all 6 workers were frozen**. Thousands of pending Fast I/O and DB requests piled up in the OS socket queue waiting for the event loops to wake up. The entire API effectively went offline.

### The Ldcorn Firewall
Ldcorn worked exactly as designed. 
The blocking ML and Math endpoints completely choked their isolated quarantine workers (dropping to 0.5 req/s). **But the `default` worker group was completely unaffected.** 
Ldcorn continued to serve Fast I/O at **950 req/s** and handled Database operations at ~100 req/s, seamlessly maintaining uptime for 90% of the application while the heavy endpoints choked.

This is the entire point of Ldcorn. It trades a tiny bit of optimal performance in perfect scenarios for **architectural resilience** in messy, real-world production environments.

NOTE: All of these can be reproduced , look at /examples
