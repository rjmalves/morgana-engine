import morgana_engine
from setuptools import setup, find_packages  # type: ignore

long_description = ""
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

requirements = []
with open("requirements.txt", "r") as fh:
    requirements = fh.readlines()

setup(
    name="morgana_engine",
    version=morgana_engine.__version__,
    author="Rogerio Alves",
    author_email="rogerioalves.ee@gmail.com",
    description="Python module for handling partitioned data using SQL queries.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rjmalves/morgana_engine",
    packages=find_packages(),
    package_data={"morgana_engine": ["py.typed"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
)
