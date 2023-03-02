from setuptools import setup

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='cbcmgr',
    version='1.1.8',
    packages=['cbcmgr'],
    url='https://github.com/mminichino/cb-util',
    license='MIT License',
    author='Michael Minichino',
    python_requires='>=3.9',
    install_requires=[
        'attrs',
        'couchbase',
        'dnspython',
        'docker',
        'pytest',
        'requests',
        'urllib3'
    ],
    author_email='info@unix.us.com',
    description='Couchbase connection manager',
    long_description=long_description,
    long_description_content_type='text/markdown'
)
