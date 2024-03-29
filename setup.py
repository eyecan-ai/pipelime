#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst", encoding='UTF-8') as readme_file:
    readme = readme_file.read()

with open("requirements.txt", encoding='UTF-8') as requirements_file:
    requirements = requirements_file.readlines()

setup_requirements = [
    "pytest-runner",
]

test_requirements = [
    "pytest>=3",
]

extras_requirements = {
    'minio': ['minio']
}

setup(
    author="daniele de gregorio",
    author_email="daniele.degregorio@eyecan.ai",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="data pipeline 101",
    entry_points={
        "console_scripts": [
            "pipelime=pipelime.cli.main:pipelime",
        ],
    },
    install_requires=requirements,
    extras_require=extras_requirements,
    license="GNU General Public License v3",
    long_description=readme,
    include_package_data=True,
    keywords="pipelime",
    name="pipelime",
    packages=find_packages(include=["pipelime", "pipelime.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/eyecan-ai/pipelime",
    version="0.1.8",
    zip_safe=False,
)
