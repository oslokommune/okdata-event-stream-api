import os

from setuptools import setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

service_name = os.path.basename(os.getcwd())

setup(
    name=service_name,
    version="0.1.0",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Event streams API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.oslo.kommune.no/origo-dataplatform/event-streams-api",
    py_modules=["app"],
    install_requires=[
        "flask==1.1.2",
        "flask-restful==0.3.8",
        "pydantic==1.5.1",
        "origo-sdk-python==0.2.3",
        "shortuuid",
        "elasticsearch-dsl==7.2.1",
        "requests-aws4auth==1.0",
    ],
)
