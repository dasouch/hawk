from setuptools import setup


install_requires = [
    'aio-pika==5.6.0',
    'ujson==3.0.0'
]


test_require = [
    'pytest==5.4.3',
    'pytest-asyncio==0.14.0',
    'pytest-cov==2.10.0',
    'pytest-mock==3.1.1'
]

setup(
    name='hawk',
    version='1.1.1',
    packages=['hawk'],
    install_requires=install_requires,
    author='Danilo Vargas',
)
