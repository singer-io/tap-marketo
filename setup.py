#!/usr/bin/env python

from setuptools import setup

setup(name='tap-marketo',
      version='0.1.1',
      description='Taps Marketo data',
      author='Stitch',
      url='https://github.com/stitchstreams/tap-marketo',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_marketo'],
      install_requires=[],
      entry_points='''
          [console_scripts]
          tap-marketo=tap_marketo:main
      ''',
      packages=['tap_marketo'],
      package_data = {
          'tap_marketo': []
          }
)