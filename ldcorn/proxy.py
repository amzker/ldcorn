import asyncio
import itertools
import posixpath
import urllib.parse
from typing import Dict
from .config import LdConfig
from .log import logger

class LdcornProxy:
    def __init__(self, config: LdConfig, group_sockets: Dict[str, list[str]]):
        self.update_config(config, group_sockets)
        
    def update_config(self, config: LdConfig, group_sockets: Dict[str, list[str]]):
        old_semaphores = getattr(self, 'semaphores', {})
        old_config = getattr(self, 'config', None)
        self.config = config
        self.group_sockets = {
            name: itertools.cycle(socks) for name, socks in group_sockets.items()
        }
        
        # Compile flat routes list and sort by descending length for longest prefix match
        self.routes = []
        self.default_group = None
        for group in self.config.workers:
            for route in group.routes:
                if route == "*":
                    self.default_group = group.name
                else:
                    self.routes.append((route, group.name))
        self.routes.sort(key=lambda x: len(x[0]), reverse=True)
        
        # for workers that have a max_req_per_worker > 0
        self.semaphores = {}
        for group in self.config.workers:
            if group.max_req_per_worker > 0:
                for sock in group_sockets[group.name]:
                    # Preserve existing semaphore if it exists and the limit is unchanged
                    if sock in old_semaphores and old_config:
                        old_group = next((g for g in old_config.workers if g.name == group.name), None)
                        if old_group and old_group.max_req_per_worker == group.max_req_per_worker:
                            self.semaphores[sock] = old_semaphores[sock]
                            continue
                    self.semaphores[sock] = asyncio.Semaphore(group.max_req_per_worker)
        
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            try:
                # Read the entire headers block up to the blank line \r\n\r\n
                headers_data = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), timeout=5.0)
            except asyncio.IncompleteReadError as e:
                headers_data = e.partial
            except Exception:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return

            if not headers_data or b'\r\n\r\n' not in headers_data:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return

            # Split into separate lines to parse path and check/modify Connection header
            lines = headers_data.split(b'\r\n')
            first_line = lines[0]

            parts = first_line.strip().split()
            if len(parts) >= 2:
                path = parts[1].decode('utf-8', errors='ignore')
            else:
                path = "/"

            # Strip absolute URI protocol and host if present (compliance with absolute URIs)
            if path.startswith("http://") or path.startswith("https://"):
                try:
                    rest = path.split("://", 1)[1]
                    if "/" in rest:
                        path = "/" + rest.split("/", 1)[1]
                    else:
                        path = "/"
                except Exception:
                    path = "/"

            # Normalize path to prevent path traversal security routing bypasses
            # e.g., /ml-pipeline/../default -> /default
            path_only = path.split("?", 1)[0].split("#", 1)[0]
            decoded_path = urllib.parse.unquote(path_only)
            normalized_path = posixpath.normpath(decoded_path)
            if not normalized_path.startswith("/"):
                normalized_path = "/" + normalized_path

            target_group = None
            for route, group_name in self.routes:
                if normalized_path.startswith(route):
                    target_group = group_name
                    break
            
            if not target_group:
                target_group = self.default_group
            
            if not target_group:
                try:
                    writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await writer.drain()
                except Exception:
                    pass
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return

            try:
                sock_path = next(self.group_sockets[target_group])
            except StopIteration:
                logger.error(f"No active sockets for worker group '{target_group}'")
                try:
                    writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                    await writer.drain()
                except Exception:
                    pass
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return
            
            sem = self.semaphores.get(sock_path)
            if sem:
                await sem.acquire()
            
            try:
                try:
                    worker_reader, worker_writer = await asyncio.open_unix_connection(sock_path)
                except Exception as e:
                    logger.error(f"Error connecting to worker {target_group} at {sock_path}: {e}")
                    try:
                        writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                        await writer.drain()
                    except Exception:
                        pass
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                    return

                # Determine if this is a WebSocket or standard Upgrade connection
                is_upgrade = False
                for line in lines[1:]:
                    line_stripped = line.strip().lower()
                    if line_stripped.startswith(b'upgrade:') or (line_stripped.startswith(b'connection:') and b'upgrade' in line_stripped):
                        is_upgrade = True
                        break

                # For standard non-upgrade connections, inject/replace Connection: close header
                # to prevent keep-alive connection reuse bypassing our path-based routing.
                if not is_upgrade:
                    has_connection_header = False
                    new_lines = [first_line]
                    for line in lines[1:]:
                        if not line:
                            continue
                        line_stripped = line.strip().lower()
                        if line_stripped.startswith(b'connection:'):
                            new_lines.append(b'Connection: close')
                            has_connection_header = True
                        else:
                            new_lines.append(line)
                    if not has_connection_header:
                        new_lines.append(b'Connection: close')
                    modified_headers = b'\r\n'.join(new_lines) + b'\r\n\r\n'
                else:
                    modified_headers = headers_data

                # Forward the already consumed headers block without draining yet to avoid packet splitting
                worker_writer.write(modified_headers)

                async def pipe(r: asyncio.StreamReader, w: asyncio.StreamWriter):
                    try:
                        while True:
                            data = await r.read(81920) # 80KB buffer
                            if not data:
                                break
                            w.write(data)
                            await w.drain()
                    except Exception:
                        pass

                # Proxy data bidirectionally, cancel the other side if one disconnects
                t1 = asyncio.create_task(pipe(reader, worker_writer))
                t2 = asyncio.create_task(pipe(worker_reader, writer))
                
                done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                    
            finally:
                if sem:
                    sem.release()
            
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, asyncio.TimeoutError, asyncio.CancelledError):
            pass
        except Exception as e:
            logger.warning(f"Proxy error: {e}")
        finally:
            try:
                if 'worker_writer' in locals():
                    worker_writer.close()
                    await worker_writer.wait_closed()
            except Exception:
                pass
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def serve(self):
        host, port_str = self.config.bind.split(":")
        port = int(port_str)
        server = await asyncio.start_server(self.handle_client, host, port)
        logger.info(f"Ldcorn master listening on http://{self.config.bind}")
        async with server:
            await server.serve_forever()
