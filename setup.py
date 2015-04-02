
from setuptools import setup

setup(
        name='bb9',
        version='0.1',
        description='Command Line Utilities for Blackboard 9',
        url='http://github.com/bmccary/bb9',
        author='Brady McCary',
        author_email='brady.mccary@gmail.com',
        license='MIT',
        packages=['bb9'],
        install_requires=[
                'csvu', # For printing human-readable CSV on console
            ],
        scripts=[
                    'bin/bb9-to-meta',
                    'bin/bb9-to-csv',
                ],
        zip_safe=False
    )
