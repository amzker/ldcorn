import argparse
import asyncio
import importlib.util
import os
import sys
import signal

from .config import LdConfig, validate_config
from .worker_manager import WorkerManager
from .proxy import LdcornProxy
from .log import logger

def load_config(config_path: str) -> LdConfig:
    abs_path = os.path.abspath(config_path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"Error: Config file {config_path} not found.")
        
    config_dir = os.path.dirname(abs_path)
    sys.path.insert(0, config_dir)
    try:
        sys.modules.pop("ldcorn_user_config", None)
        spec = importlib.util.spec_from_file_location("ldcorn_user_config", abs_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Error: Could not load config file {config_path}.")
            
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            raise RuntimeError(f"Error executing config file {config_path}: {e}")
        
        if not hasattr(module, "config"):
            raise ValueError(f"Error: Config file {config_path} must define a 'config' variable of type LdConfig.")
            
        config = module.config
        if not isinstance(config, LdConfig):
            raise TypeError(f"Error: 'config' variable in {config_path} must be an instance of LdConfig.")
            
        validate_config(config)
        return config
    finally:
        if config_dir in sys.path:
            sys.path.remove(config_dir)

async def main_async(config_path: str):
    config = load_config(config_path)
    manager = WorkerManager(config)
    proxy = None
    is_reloading = False
    
    def handle_sighup():
        nonlocal is_reloading
        if not is_reloading:
            is_reloading = True
            asyncio.create_task(reload_all())
        else:
            logger.warning("Ignoring SIGHUP: Reload already in progress.")
        
    async def reload_all():
        nonlocal config, is_reloading
        logger.info("Reload signal received! Re-evaluating configuration...")
        try:
            config = load_config(config_path)
            new_sockets = await manager.reload(config)
            if proxy:
                proxy.update_config(config, new_sockets)
            logger.info("Zero-downtime reload complete!")
        except Exception as e:
            logger.error(f"Failed to reload config: {e}")
        finally:
            is_reloading = False

    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGHUP, handle_sighup)
    except NotImplementedError:
        pass # Windows

    try:
        await manager.start()
        proxy = LdcornProxy(config, manager.group_sockets)
        await proxy.serve()
    except KeyboardInterrupt:
        pass
    finally:
        await manager.stop()

def main():
    parser = argparse.ArgumentParser(description="Ldcorn - Path-based routing proxy for Uvicorn")
    parser.add_argument("-c", "--config", required=True, help="Path to Python configuration file (e.g., ldconfig.py)")
    args = parser.parse_args()
    
    try:
        asyncio.run(main_async(args.config))
    except KeyboardInterrupt:
        logger.info("Exiting ldcorn.")
    except Exception as e:
        logger.critical(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
