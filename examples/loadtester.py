import argparse
import asyncio
import time
import random
from collections import Counter
import aiohttp

def parse_duration(d_str: str) -> int:
    if not d_str: return 0
    d_str = d_str.lower()
    if d_str.endswith('m'): return int(d_str[:-1]) * 60
    elif d_str.endswith('h'): return int(d_str[:-1]) * 3600
    elif d_str.endswith('s'): return int(d_str[:-1])
    else: return int(d_str)

async def fetch_endpoint(session: aiohttp.ClientSession, url: str, method: str = "GET", json_data: dict = None):
    start_time = time.time()
    try:
        if method == "POST":
            async with session.post(url, json=json_data, timeout=aiohttp.ClientTimeout(total=60)) as response:
                try:
                    body = await response.json()
                except Exception:
                    body = await response.text()
                elapsed = time.time() - start_time
                return response.status, elapsed, None, body
        else:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                try:
                    body = await response.json()
                except Exception:
                    body = await response.text()
                elapsed = time.time() - start_time
                return response.status, elapsed, None, body
    except asyncio.TimeoutError:
        return 0, 0.0, "Timeout", None
    except aiohttp.ClientError as e:
        return 0, 0.0, f"ClientError: {str(e) or type(e).__name__}", None
    except Exception as e:
        return 0, 0.0, f"Error: {type(e).__name__}", None

async def load_test_endpoint(
    endpoint_name: str, 
    url: str, 
    requests: int, 
    concurrency: int, 
    duration: int = 0,
    method: str = "GET",
    json_data_factory = None,
    url_factory = None
):
    mode_text = f"for {duration} seconds" if duration > 0 else f"for {requests} requests"
    print(f"[{endpoint_name}] Starting load test: {mode_text}, {concurrency} concurrent")
    
    results = []
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(concurrency)
        start_total = time.time()
        
        async def bounded_worker():
            local_results = []
            if duration > 0:
                while time.time() - start_total < duration:
                    async with sem:
                        req_url = url_factory() if url_factory else url
                        req_json = json_data_factory() if json_data_factory else None
                        local_results.append(await fetch_endpoint(session, req_url, method, req_json))
            else:
                async with sem:
                    req_url = url_factory() if url_factory else url
                    req_json = json_data_factory() if json_data_factory else None
                    local_results.append(await fetch_endpoint(session, req_url, method, req_json))
            return local_results

        if duration > 0:
            # Spawn 'concurrency' number of infinite looping workers
            tasks = [asyncio.create_task(bounded_worker()) for _ in range(concurrency)]
        else:
            # Spawn 'requests' number of single-shot workers
            tasks = [asyncio.create_task(bounded_worker()) for _ in range(requests)]
            
        worker_results = await asyncio.gather(*tasks)
        for r_list in worker_results:
            results.extend(r_list)
            
        total_time = time.time() - start_total

    successes = [r for r in results if r[0] == 200 or r[0] == 201]
    failures = [r for r in results if r[0] != 200 and r[0] != 201]
    
    if successes:
        latencies = [r[1] for r in successes]
        avg_time = sum(latencies) / len(latencies)
        max_time = max(latencies)
        min_time = min(latencies)
        sample_body = successes[0][3]
    else:
        avg_time = max_time = min_time = 0.0
        sample_body = None
        
    print(f"\n" + "="*50)
    print(f"   Results: {endpoint_name}")
    print(f"   {url}")
    print("="*50)
    print(f"Total Requests: {len(results)}")
    print(f"Successful:     {len(successes)}")
    print(f"Failed:         {len(failures)}")
    print(f"Total Time:     {total_time:.2f} seconds")
    print(f"Requests/sec:   {len(results) / total_time:.2f}")
    if successes:
        print(f"Avg Latency:    {avg_time:.4f} seconds")
        print(f"Min Latency:    {min_time:.4f} seconds")
        print(f"Max Latency:    {max_time:.4f} seconds")
        print(f"Sample Payload: {str(sample_body)[:100]}...")
    
    if failures:
        print("\nFailure Reasons:")
        reasons = Counter([f[2] or f"HTTP {f[0]}" for f in failures])
        for reason, count in reasons.items():
            print(f" - {reason}: {count} times")


async def send_raw_payload(host: str, port: int, payload: bytes):
    try:
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(payload)
        await writer.drain()
        
        # Determine if we should abruptly close without reading
        if b"Slowloris" in payload or b"AbruptClose" in payload:
            writer.close()
            await writer.wait_closed()
            return b"Abruptly closed"
            
        resp = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        writer.close()
        await writer.wait_closed()
        return resp
    except Exception as e:
        return str(e).encode()

async def raw_tcp_loop(host: str, port: int, concurrency: int, duration: int = 0, requests: int = 0):
    mode_text = f"for {duration} seconds" if duration > 0 else f"for {requests} requests"
    print(f"[Raw TCP Edge Cases] Starting raw TCP parser tests: {mode_text}, {concurrency} concurrent")
    
    payloads = [
        # 1. Path traversal bypass attempt via URL encoding
        b"GET /ml-pipeline/..%2fdefault HTTP/1.1\r\nHost: localhost\r\n\r\n",
        
        # 2. HTTP Request Line Whitespace Fragility
        b"GET    /    HTTP/1.1\r\nHost: localhost\r\n\r\n",
        
        # 3. Absolute URI compliance test
        b"GET http://localhost:8000/ml-pipeline/start HTTP/1.1\r\nHost: localhost\r\n\r\n",
        
        # 4. Slowloris / Abrupt close (Early client disconnect)
        b"GET / AbruptClose HTTP/1.1\r\nHost: localhost\r\n",
        
        # 5. Invalid connection header multiplexing
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive, Upgrade, something-weird\r\n\r\n",
        
        # 6. Method unrecognized
        b"GARBAGE / HTTP/1.1\r\nHost: localhost\r\n\r\n",
    ]
    
    results = []
    sem = asyncio.Semaphore(concurrency)
    start_total = time.time()
    
    async def tcp_stress_worker():
        local_results = []
        if duration > 0:
            while time.time() - start_total < duration:
                async with sem:
                    payload = random.choice(payloads)
                    resp = await send_raw_payload(host, port, payload)
                    local_results.append(resp)
        else:
            async with sem:
                payload = random.choice(payloads)
                resp = await send_raw_payload(host, port, payload)
                local_results.append(resp)
        return local_results

    if duration > 0:
        tasks = [asyncio.create_task(tcp_stress_worker()) for _ in range(concurrency)]
    else:
        tasks = [asyncio.create_task(tcp_stress_worker()) for _ in range(requests)]
        
    worker_results = await asyncio.gather(*tasks)
    for r_list in worker_results:
        results.extend(r_list)
        
    total_time = time.time() - start_total
    
    print(f"\n" + "="*50)
    print(f"   Results: Raw TCP Edge Cases")
    print("="*50)
    print(f"Total TCP Requests:    {len(results)}")
    print(f"Total Time:            {total_time:.2f} seconds")
    print(f"Requests/sec:          {len(results) / total_time:.2f}")


async def main_async(requests: int, concurrency: int, duration_str: str):
    host = "127.0.0.1"
    port = 8000
    base_url = f"http://{host}:{port}"
    
    duration = parse_duration(duration_str)
    
    print(f"Starting Ldcorn Load & Stress Tester")
    print(f"Target: {base_url}\n")
    
    ml_requests = max(1, requests // 10)
    ml_concurrency = max(1, concurrency // 10)
    
    tasks = [
        load_test_endpoint("Fast I/O Endpoint", f"{base_url}/", requests, concurrency, duration),
        load_test_endpoint("Stateful Counter", f"{base_url}/counter", ml_requests, ml_concurrency, duration),
        load_test_endpoint("Heavy ML Endpoint", f"{base_url}/ml-pipeline/start", ml_requests, ml_concurrency, duration),
        load_test_endpoint("DB Read Endpoint", f"{base_url}/db/read", ml_requests, ml_concurrency, duration),
        load_test_endpoint(
            "DB Insert Endpoint", 
            f"{base_url}/db/insert", 
            ml_requests, 
            ml_concurrency, 
            duration, 
            method="POST", 
            json_data_factory=lambda: {"name": f"User_{random.randint(100000, 999999)}"}
        ),
        load_test_endpoint(
            "Math Prime Endpoint", 
            f"{base_url}/math/prime/1000", 
            ml_requests, 
            ml_concurrency, 
            duration,
            url_factory=lambda: f"{base_url}/math/prime/{random.randint(1000, 5000)}"
        ),
        raw_tcp_loop(host, port, ml_concurrency, duration, ml_requests)
    ]
    
    await asyncio.gather(*tasks)

def main():
    parser = argparse.ArgumentParser(description="Ldcorn Stress & Load Tester")
    parser.add_argument("-n", "--requests", type=int, default=1000, help="Number of total requests for the fast endpoint if duration is not set.")
    parser.add_argument("-c", "--concurrency", type=int, default=50, help="Number of concurrent requests (default: 50).")
    parser.add_argument("-d", "--duration", type=str, default="", help="Run for a specific duration instead of request count (e.g., '60s', '10m', '1h').")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(main_async(args.requests, args.concurrency, args.duration))
    except KeyboardInterrupt:
        print("\nTest cancelled by user.")

if __name__ == "__main__":
    main()
