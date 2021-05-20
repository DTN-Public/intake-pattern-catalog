from pathlib import Path

from pkg_resources import parse_requirements
from setuptools import find_packages, setup

requirements = Path("./requirements.txt").read_text()


def get_version_and_cmdclass(package_path):
    """Load version.py module without importing the whole package.

    Template code from miniver
    """
    import os
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("version", os.path.join(package_path, "_version.py"))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__version__, module.cmdclass


version, cmdclass = get_version_and_cmdclass("./src/intake_patterncatalog")


setup(
    name="dtn-intake-patterncatalog",
    description="",
    author="Tim Hopper",
    author_email="tim.hopper@dtn.com",
    install_requires=[str(r) for r in parse_requirements(requirements)],  # type: ignore
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    zip_safe=False,
    entry_points={
        "intake.drivers": [
            "pattern_cat = intake_patterncatalog:PatternCatalog",
        ]
    },
    version=version,
    cmdclass=cmdclass,
)
