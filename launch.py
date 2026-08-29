"""
Launch all A2A agents as independent processes.

Usage:
    python3 launch.py

All agents start in the background. Press Ctrl+C to stop all.

Ports:
    8100  Orchestrator
    8101  Architect
    8102  Developer Lead
    8103  Developer
    8104  Ona
    8105  Registry
"""
import multiprocessing
import signal
import sys
import time

import uvicorn

AGENTS = [
    # Registry first so others can self-register on startup
    ("a2a.agents.registry:app",     8105, "Registry"),
    ("a2a.agents.architect:app",    8101, "Architect"),
    ("a2a.agents.dev_lead:app",     8102, "Developer Lead"),
    ("a2a.agents.developer:app",    8103, "Developer"),
    ("a2a.agents.ona:app",          8104, "Ona"),
    ("a2a.agents.orchestrator:app", 8100, "Orchestrator"),
]


def _run(module: str, port: int, name: str):
    print(f"  ▶ {name} agent starting on :{port}")
    uvicorn.run(module, host="0.0.0.0", port=port, log_level="warning")


def main():
    print("\n🚀 Starting MuleSoft A2A Onboarding Workbench\n")

    processes = []
    for module, port, name in AGENTS:
        p = multiprocessing.Process(target=_run, args=(module, port, name), daemon=True)
        p.start()
        processes.append((p, name, port))
        time.sleep(0.4)   # stagger startup so registry is ready first

    print("\n✅ All agents running:\n")
    for _, name, port in processes:
        print(f"   {name:20s}  http://localhost:{port}")
    print(f"\n   Registry listing:    http://localhost:8105/agents")
    print(f"   Orchestrator health: http://localhost:8100/health")
    print("\nPress Ctrl+C to stop all agents.\n")

    def _shutdown(sig, frame):
        print("\n⏹  Shutting down all agents...")
        for p, _, _ in processes:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for p, _, _ in processes:
        p.join()


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    main()
