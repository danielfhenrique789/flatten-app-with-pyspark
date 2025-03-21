import os
import importlib
import pkgutil

__all__ = []

# Dynamically load all *_schema from modules in this package
package_dir = os.path.dirname(__file__)

for _, module_name, _ in pkgutil.iter_modules([package_dir]):
    if module_name.startswith("_"):
        continue  # skip __init__ or other private files

    module = importlib.import_module(f"schemas.{module_name}")
    for attr in dir(module):
        if attr.endswith("_schema"):
            globals()[attr] = getattr(module, attr)
            __all__.append(attr)