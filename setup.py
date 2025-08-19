#!/usr/bin/env python

from setuptools import setup

setup(name='tap-marketo',
      version='2.6.5',
      description='Singer.io tap for extracting data from the Marketo API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_marketo'],
      install_requires=[
          'singer-python==6.0.1',
          'requests==2.32.4',
          'pendulum==1.2.0',
          'backoff==2.2.1',
      ],
      extras_require={
          'dev': [
              'ipdb',
          ],
          'test': [
              'freezegun==1.5.1',
              'requests_mock==1.12.1',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-marketo=tap_marketo:main
      ''',
      packages=['tap_marketo'],
      package_data = {
          'tap_marketo/schemas': [
              "activity_types.json",
              "campaigns.json",
              "programs.json",
          ]
      },
      include_package_data=True,
)
