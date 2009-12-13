#!/usr/bin/env python
'''
Installs pymodbus using distutils

Run:
    python setup.py install
to install the package from the source archive.

For information about setuptools
http://peak.telecommunity.com/DevCenter/setuptools#new-and-changed-setup-keywords
'''
try: # if not installed, install and proceed
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()

from distutils.core import Command
import sys, os

#---------------------------------------------------------------------------# 
# Extra Commands
#---------------------------------------------------------------------------# 
command_classes = {}

class BuildApiDocs(Command):
    ''' Helper command to build the available api documents
    This scans all the subdirectories under api and runs the
    build.py script underneath trying to build the api
    documentation for the given format.
    '''
    user_options = []

    def initialize_options(self):
        ''' options setup '''
        pass

    def finalize_options(self):
        ''' options teardown '''
        pass

    def run(self):
        ''' command runner '''
        old_cwd = os.getcwd()
        for entry in os.listdir('./api'):
            os.chdir('./api/%s' % entry)
            os.system('python build.py')
            os.chdir(old_cwd)

command_classes['build_apidocs'] = BuildApiDocs

#---------------------------------------------------------------------------# 
# Configuration
#---------------------------------------------------------------------------# 
#from mamba import __version__

setup(name  = 'mamba',
    version = "0.1",
    #version = __version__,
    description = "A port of ruby starling to python",
    long_description='''
    Starling is a powerful but simple messaging server that enables reliable
    distributed queuing with an absolutely minimal overhead. It speaks the
    MemCache protocol for maximum cross-platform compatibility. Any language
    that speaks MemCache can take advantage of Starling's queue facilities.
    So all that...in python.
    ''',
    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: System :: Networking',
        'Topic :: Utilities'
    ],
    keywords = 'message, queue, twisted',
    author = 'Galen Collins',
    author_email = 'bashwork@gmail.com',
    maintainer = 'Galen Collins',
    maintainer_email = 'bashwork@gmail.com',
    url='http://github.com/bashwork/mamba/',
    license = 'LGPL',
    packages = find_packages(exclude=['ez_setup', 'tests', 'doc']),
    platforms = ["Linux","Mac OS X","Win"],
    include_package_data = True,
    zip_safe = True,
    install_requires = [
        'twisted >= 2.5.0',
        'nose >= 0.9.3',
        'python-memcached >= 1.45',
        'pyyaml >= 3.09',
    ],
    test_suite = 'nose.collector',
    cmdclass = command_classes,
)
