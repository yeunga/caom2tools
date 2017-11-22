#!/usr/bin/env python
# Licensed under a 3-clause BSD style license - see LICENSE.rst

import glob
import os
import sys
import imp
from setuptools.command.test import test as TestCommand
from setuptools import find_packages

from setuptools import setup

import distutils.cmd
import distutils.log
import subprocess

# read the README.rst file and return as string.
def readme():
    with open('README.rst') as r_obj:
        return r_obj.read()

# Get some values from the setup.cfg
try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

conf = ConfigParser()
conf.optionxform=str
conf.read(['setup.cfg'])
metadata = dict(conf.items('metadata'))

PACKAGENAME = metadata.get('package_name', 'packagename')
DESCRIPTION = metadata.get('description', 'CADC package')
AUTHOR = metadata.get('author', 'CADC')
AUTHOR_EMAIL = metadata.get('author_email', 'cadc@nrc.gc.ca')
LICENSE = metadata.get('license', 'unknown')
URL = metadata.get('url', 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca')

# VERSION should be PEP386 compatible (http://www.python.org/dev/peps/pep-0386)
VERSION = metadata.get('version', 'none')

# generate the version file
with open(os.path.join(PACKAGENAME, 'version.py'), 'w') as f:
    f.write('version = \'{}\'\n'.format(VERSION))	

# Treat everything in scripts except README.rst as a script to be installed
scripts = [fname for fname in glob.glob(os.path.join('scripts', '*'))
           if os.path.basename(fname) != 'README.rst']

# Define entry points for command-line scripts
entry_points = {'console_scripts': []}

entry_point_list = conf.items('entry_points')
for entry_point in entry_point_list:
    entry_points['console_scripts'].append('{0} = {1}'.format(entry_point[0],
                                                              entry_point[1]))

# add the --cov option to the test command
class PyTest(TestCommand):
    """class py.test for the testing

    """
    user_options = []

    def __init__(self, dist, **kw):
        TestCommand.__init__(self, dist, **kw)
        self.pytest_args = ['--cov', PACKAGENAME]

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        err_no = pytest.main(self.pytest_args)
        sys.exit(err_no)
        
class IntTestCommand(distutils.cmd.Command):
  """A custom command to run integration tests."""

  description = 'Integration tests'
  user_options = []
  
  def initialize_options(self):
    """Set default values for options."""
    # Each user option must be listed here with their default value.

  def finalize_options(self):
    """Post-process options."""

  def run(self):
    """Run command."""
    import pytest
    testfile = os.getcwd() + '/tests/test-integration.py'
    pytest.main(['-s', '--capture=no','-x', testfile])
    #inttests = imp.load_source("tests", testfile)
    #inttestattr = getattr(inttests, "TestCaom2Integration")()
    #self.announce(
    #    'Running inttests: {}'.format(testfile),
    #    level=distutils.log.INFO)
    #inttestattr.runTest()

# Note that requires and provides should not be included in the call to
# ``setup``, since these are now deprecated. See this link for more details:
# https://groups.google.com/forum/#!topic/astropy-dev/urYO8ckB2uM

setup(name=PACKAGENAME,
      version=VERSION,
      description=DESCRIPTION,
      scripts=scripts,
      install_requires=metadata.get('install_requires', '').strip().split(),
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license=LICENSE,
      url=URL,
      long_description=readme(),
      zip_safe=False,
      use_2to3=False,
      setup_requires=['pytest-runner'],
      entry_points=entry_points,
      packages=find_packages(),
      package_data={PACKAGENAME: ['data/*', 'tests/data/*', '*/data/*', '*/tests/data/*']},
      classifiers=[
        'Natural Language :: English',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
      ],
      cmdclass = {
          'coverage': PyTest,
          'inttest': IntTestCommand
      }
)
