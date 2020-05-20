from setuptools import setup


install_requires = [
    'aio-pika==6.6.0',
    'ujson==2.0.3'
]


test_require = [
    'pytest==5.4.2',
    'pytest-asyncio==0.12.0',
    'pytest-cov==2.8.1',
    'pytest-mock==3.1.0'
]

setup(
    name='hawk',
    version='1.0.2',
    packages=['hawk'],
    install_requires=install_requires,
    author='Danilo Vargas',
)
