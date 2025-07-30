#!/usr/bin/env python3

from setuptools import setup

setup(name='tap-amplitude',
      version='1.2.0',
      description='Singer.io tap for extracting data from Amplitude via Snowflake',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_amplitude'],
      install_requires=[
          'snowflake-connector-python==3.15.0',
          'attrs==25.3.0',
          'pendulum==3.1.0',
          'pytz==2025.2',
          'singer-python==6.1.1',
          'backoff==2.2.1'
      ],
      entry_points='''
          [console_scripts]
          tap-amplitude=tap_amplitude:main
      ''',
      packages=['tap_amplitude', 'tap_amplitude.sync_strategies'],
)
