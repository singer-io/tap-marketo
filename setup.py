#!/usr/bin/env python

import sys
from setuptools import setup
from setuptools.command.install import install
from setuptools.command.develop import develop

class CustomInstallCommand(install):
    def run(self):
        print("Don't do this. Instead do:")
        print("\tpip install .")
        sys.exit(1)


class CustomDevelopCommand(develop):
    def run(self):
        print("Don't do this. Instead do:")
        print("\tpip install -e .")
        sys.exit(1)


setup(name='tap-marketo',
      version='2.0.7',
      description='Singer.io tap for extracting data from the Marketo API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_marketo'],
      install_requires=[
          'singer-python==4.0.2',
          'requests==2.12.4',
          'pendulum==1.2.0',
          'freezegun>=0.3.9',
          'requests_mock>=1.3.0'
      ],
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
      cmdclass={
          'install': CustomInstallCommand,
          'develop': CustomDevelopCommand,
      },
)
