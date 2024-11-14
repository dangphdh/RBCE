from setuptools import setup, find_packages

setup(
    name="databricks-etl-project",
    version="0.1.0",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pyspark>=3.3.2',
        'delta-spark>=2.2.0',
        'pandas>=2.0.1',
        'python-dotenv>=1.0.0'
    ],
    extras_require={
        'dev': [
            'pytest>=7.3.1',
            'flake8>=3.9.0',
            'black>=22.3.0'
        ]
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="A modular ETL project for Databricks",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/databricks-etl-project",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
