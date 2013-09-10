from setuptools import find_packages, setup

setup(
    name='Whip',
    version='0.1',
    packages=find_packages(exclude=['tests']),
    entry_points={
        'console_scripts': [
            'whip-cli = whip.cli:main',
        ],
    }
)
