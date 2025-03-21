import importlib
import re
import os
from base.spark_job import SparkJob

def convert_class_to_module(class_name: str) -> str:
    """Convert CamelCase class name to snake_case module name."""
    return re.sub(r'(?<!^)(?=[A-Z])', '_', class_name).lower()

def resolve_job_file(job_name: str, jobs_dir="src/jobs") -> str:
    """
    Determines the correct job module filename from a given CamelCase job name.
    """
    full_snake = convert_class_to_module(job_name)
    full_path = os.path.join(jobs_dir, f"{full_snake}.py")
    #raise Exception(f"********** {full_path}")
    if os.path.isfile(full_path):
        return job_name  # Exact match

    # Try partial match using first CamelCase component
    match = re.match(r'^([A-Z][a-z0-9]*)', job_name)
    if match:
        first_part = match.group(1)
        partial_snake = convert_class_to_module(first_part)
        partial_path = os.path.join(jobs_dir, f"{partial_snake}.py")

        if os.path.isfile(partial_path):
            return first_part

    raise Exception(f"No matching job file found for '{job_name}' in {jobs_dir}")

def get_job_class(job_name: str):
    """Dynamically loads the job class based on job_name"""
    try:
        _job_name = resolve_job_file(job_name)
        module_name = convert_class_to_module(_job_name)  # Convert CamelCase to snake_case
        module_path = f"jobs.{module_name}"  # Adjusted module path
        print("Module: ",module_path)
        module = importlib.import_module(module_path)  # Import the module
        job_class = getattr(module, _job_name)
        if issubclass(job_class, SparkJob):
            return job_class
        else:
            raise ValueError(f"{job_name} is not a valid SparkJob class")
    except (ModuleNotFoundError, AttributeError) as e:
        raise ValueError(f" ERROR: Job '{job_name}' or '{_job_name}' not found! {e}")
