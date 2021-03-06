#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-capterra",
    version="0.1.0",
    description="Singer.io tap for extracting data from Capterra",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_capterra"],
    install_requires=["singer-python>=5.0.12", "requests", "ratelimit", "tqdm"],
    entry_points="""
    [console_scripts]
    tap-capterra=tap_capterra:main
    """,
    packages=["tap_capterra"],
    package_data={"schemas": ["tap_capterra/schemas/*.json"]},
    include_package_data=True,
)
