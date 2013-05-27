#!/usr/bin/env python
# coding: utf8

VERSION = '0.1'

import sys
import os
from setuptools import setup, find_packages
from setuptools.extension import Extension

extra_args = {}
if (sys.version_info[0] >= 3):
    extra_args['use_2to3'] = True


setup(name='finamdownloader',
  version=VERSION,
  description='finamdownloader',
  author='Serg Shabalin',
  url='https://sergshabal@bitbucket.org/sergshabal/finamdownloader.git',
  packages=['finamdownloader'],
  install_requires=['numpy', 'pandas'],
  **extra_args
)