from pathlib import Path

from pkg_resources import parse_requirements
from setuptools import find_packages, setup

requirements = Path("./requirements.txt").read_text()

setup(
    name="intake-patterncatalog",
    version="2021.4.0",
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
)
