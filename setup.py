from setuptools import setup
import cbcmgr
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='cbcmgr',
    version=cbcmgr.__version__,
    packages=['cbcmgr'],
    url='https://github.com/mminichino/cb-util',
    license='MIT License',
    author='Michael Minichino',
    python_requires='>=3.8',
    entry_points={
        'console_scripts': [
            'cbcutil = cbcmgr.cli.cbcutil:main',
        ]
    },
    package_data={'cbcmgr': ['data/*']},
    install_requires=[
        "attrs>=22.2.0",
        "couchbase==4.1.5",
        "dnspython>=2.3.0",
        "docker>=5.0.3",
        "pytest>=7.0.1",
        "pytest-asyncio>=0.16.0",
        "requests>=2.31.0",
        "urllib3==1.26.16",
        "xmltodict>=0.13.0",
        "bumpversion>=0.6.0",
        "overrides>=7.4.0",
        "Jinja2==3.1.2",
        "pandas>=1.5.3",
        "numpy>=1.24.3",
        "Pillow>=8.4.0"
    ],
    author_email='info@unix.us.com',
    description='Couchbase connection manager',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=["couchbase", "nosql", "pycouchbase", "database"],
    classifiers=[
          "Development Status :: 5 - Production/Stable",
          "License :: OSI Approved :: MIT License",
          "Intended Audience :: Developers",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Topic :: Database",
          "Topic :: Software Development :: Libraries",
          "Topic :: Software Development :: Libraries :: Python Modules"],
)
