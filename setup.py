#!/usr/bin/env python

from setuptools import setup

setup(name='tap-marketo',
      version='0.4.4',
      description='Singer.io tap for extracting data from the Marketo API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_marketo'],
      install_requires=[
          'singer-python>=0.1.0',
          'requests==2.12.4',
          'backoff==1.3.2',
      ],
      entry_points='''
          [console_scripts]
          tap-marketo=tap_marketo:main
      ''',
      packages=['tap_marketo'],
      package_data = {
          'tap_marketo/schemas': [
              "activities.json",
              "activity_types.json",
              "lists.json",
          ]
      },
      include_package_data=True,
)
