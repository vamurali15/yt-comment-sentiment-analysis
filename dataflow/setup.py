from setuptools import setup, find_packages

setup(
    name="yt_comments_pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]==2.49.0",
        "google-cloud-pubsub==3.16.2",
        "google-cloud-bigquery==3.11.0",
        "PyYAML==6.1"
    ],
    entry_points={
        'console_scripts': [
            'run-pipeline = pipeline:run',
        ],
    },
)