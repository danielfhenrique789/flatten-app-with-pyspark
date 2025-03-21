import json


def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)


def get_config(config, job_name):
    if "jobs" in config.keys():
        if job_name in config["jobs"].keys():
            job_config = config["jobs"][job_name]
        else:
            raise Exception(f"There is no {job_name} job in the config file.")
    else:
        raise Exception("Invalid Config file.")

    if "db" in job_config.keys():
        if job_config["db"] in config["databases"].keys():
            job_config["db"] = config["databases"][job_config["db"]]

    return job_config