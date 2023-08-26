from setuptools import setup
import cbcmgr
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='cbcmgr',
    version=cbcmgr.__version__,
    packages=['cbcmgr'],
    url='https://github.com/mminichino/cb-util',
    license='MIT License',
    author='Michael Minichino',
    python_requires='>=3.8',
    install_requires=required,
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
