from setuptools import setup


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="ETL",
    version="0.0.1",
    description="A basic ETL implementation for Customer Data"
    package_dir="main.py",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/",
    author="Blessing Agadagba",
    author_email="agadagbablessing@gmail.com",

    install_requires=[
        "python >= 3.8"
        "pandas ~= 1.3.0",
    ]
)
