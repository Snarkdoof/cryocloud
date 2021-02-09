from CryoCore import API

cfg = API.get_config("feskslog")
status = API.get_status("feskslog")
log = API.get_log("feskslog")

cfg.set_default("f00", "bar")
status["state"] = "Starting"
log.debug("A debug statement")
log.warning("Cool enough")
status["fu"] = cfg["f00"]
status["sate"] = "Done"
