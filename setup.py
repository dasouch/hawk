from setuptools import setup


install_requires = [
    'ujson==5.4.0'
]


test_require = [
    'pytest==5.4.3',
    'pytest-asyncio==0.14.0',
    'pytest-cov==2.10.0',
    'pytest-mock==3.1.1'
]

setup(
    name='hawk',
    version='1.3.5',
    packages=['hawk'],
    install_requires=install_requires,
    author='Danilo Vargas',
    dependency_links=[
        'https://github.com/mosquito/aio-pika@master#egg=aio_pika'
    ]
)
