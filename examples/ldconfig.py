from ldcorn.config import LdConfig, WorkerGroup

config = LdConfig(
    bind="127.0.0.1:8000",
    workers=[
        WorkerGroup(
            name="default",
            app="examples.app:app",
            instances=2,
            max_req_per_worker=100,
            routes=["*"],
            reload_on_sighup=False,
            uvicorn_log_level="error"
        ),
        WorkerGroup(
            name="ml",
            app="examples.app:app",
            instances=2,
            max_req_per_worker=50,
            routes=["/ml-pipeline"],
            reload_on_sighup=True,
            uvicorn_log_level="error"
        ),
        WorkerGroup(
            name="websocket_stateful",
            app="examples.app:app",
            instances=1,
            max_req_per_worker=0,
            routes=["/ws", "/counter"],
            reload_on_sighup=False,
            uvicorn_log_level="error"
        )
    ]
)
