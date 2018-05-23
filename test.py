        
from CryoCore import API

try:
    cfg = API.get_config("Dashboard")

    print("Setting default config")
    if 1:
        cfg.set_default("params.cpu_user.source", "NodeController.*")
        cfg.set_default("params.cpu_user.name", "cpu.user")
        cfg.set_default("params.cpu_user.title", "User")
        cfg.set_default("params.cpu_user.type", "Resource")

        cfg.set_default("params.cpu_system.source", "NodeController.*")
        cfg.set_default("params.cpu_system.name", "cpu.system")
        cfg.set_default("params.cpu_system.title", "System")
        cfg.set_default("params.cpu_system.type", "Resource")

        cfg.set_default("params.cpu_idle.source", "NodeController.*")
        cfg.set_default("params.cpu_idle.name", "cpu.idle")
        cfg.set_default("params.cpu_idle.title", "Idle")
        cfg.set_default("params.cpu_idle.type", "Resource")

        cfg.set_default("params.memory.source", "NodeController.*")
        cfg.set_default("params.memory.name", "memory.available")
        cfg.set_default("params.memory.title", "Free memory")
        cfg.set_default("params.memory.type", "Resource")

        cfg.set_default("params.progress.source", "Worker.*")
        cfg.set_default("params.progress.name", "progress")
        cfg.set_default("params.progress.type", "Worker")

        cfg.set_default("params.state.source", "Worker.*")
        cfg.set_default("params.state.name", "state")
        cfg.set_default("params.state.type", "Worker")

    # cfg.set_default("deleteme.fesk", True)
    print("Setting default config OK")
finally:
    API.shutdown()
