from setuptools import find_packages, setup

setup(
    name='whip',
    version='0.1',
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
    }
)
