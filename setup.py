from setuptools import setup, find_packages

setup(
    name='streampatrol',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark==3.3.0',
        'kafka-python-ng==2.2.2',
        'pandas==2.2.2',
        'prettytable==3.11.0'
    ],
    entry_points={
        'console_scripts': [
            'kafka-delta-delivery=kafka_delta_delivery:main',
            'delta-table-reader=delta_table_reader:main',
            'streaming-demo=streaming_demo:main'
        ]
    },
)