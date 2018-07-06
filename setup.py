#!/usr/bin/env python3

from setuptools import setup

setup(name='tap-amplitude',
      version='0.0.6',
      description='Singer.io tap for extracting data from Amplitude via Snowflake',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_amplitude'],
      install_requires=[
          'snowflake-connector-python==1.6.3',
          'attrs==16.3.0',
          'pendulum==1.2.0',
          'pytz==2018.4',
          'singer-python==5.1.5',
          'backoff==1.3.2',
          'nose==1.3.7'
      ],
      entry_points='''
          [console_scripts]
          tap-amplitude=tap_amplitude:main
      ''',
      packages=['tap_amplitude', 'tap_amplitude.sync_strategies'],
)
