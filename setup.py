from setuptools import setup

setup(
    name='cbutil',
    version='1.0.0',
    packages=['cbutil'],
    url='https://github.com/mminichino/cb-util',
    license='MIT License',
    author='Michael Minichino',
    python_requires='>=3.9',
    install_requires=[
        'attr',
        'attrs',
        'couchbase',
        'dnspython',
        'docker',
        'pytest',
        'requests'
    ],
    author_email='info@unix.us.com',
    description='Couchbase connection manager'
)
