from setuptools import setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="okdata-event-stream-api",
    version="0.1.0",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Event stream API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oslokommune/okdata-event-stream-api",
    py_modules=["app"],
    install_requires=[
        "boto3>=1.17",
        "elasticsearch-dsl==7.2.1",
        "fastapi>=0.65.2",
        "mangum==0.10.0",
        "okdata-aws>=1.0.0",
        "okdata-resource-auth",
        "okdata-sdk>=0.8.1",
        "pytz",
        "requests",
        "requests-aws4auth==1.0",
        "shortuuid",
        "simplejson",
    ],
)
