import os

from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as fp:
    long_description = fp.read()

setup(
    name='whip',
    description="Whip, the who, what, where and when about IP address data",
    long_description=long_description,
    version='0.1',
    author="Wouter Bolsterlee",
    author_email="uws@xs4all.nl",
    url='https://github.com/wbolster/whip',
    license='BSD',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'aaargh',
        'Flask',
        'msgpack-python',
        'plyvel',
        'ujson',
    ],
    entry_points={
        'console_scripts': [
            'whip-cli = whip.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
    ],
)
