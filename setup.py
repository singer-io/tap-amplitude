#!/usr/bin/env python3

from setuptools import setup

setup(name='tap-amplitude',
      version='1.4.1',
      description='Singer.io tap for extracting data from Amplitude via Snowflake',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'snowflake-connector-python==4.7.1',
          'pendulum==3.2.0',
          'singer-python==6.8.0',
          'backoff==2.2.1'
      ],
      entry_points='''
          [console_scripts]
          tap-amplitude=tap_amplitude:main
      ''',
      packages=['tap_amplitude', 'tap_amplitude.sync_strategies'],
)
