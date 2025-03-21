def get_job_name(arg):
    if len(arg) < 2:
        raise ValueError("ERROR: No job specified! Usage: spark-submit main.py <JobName>")

    return arg[1]