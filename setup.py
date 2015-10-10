# !/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name='usergrid-data-loader',
    version='0.0.1',
    description='Utils for loading data to Usergrid',
    packages=find_packages(),

    author=u'Jeffrey West',
    author_email='west.jeff@gmail.com',
    maintainer=u'Jeffrey West',
    maintainer_email='west.jeff@gmail.com',

    url='usergrid.apache.org',

    license='Apache 2.0',
    platforms='any',

    install_requires=[
        'boto >= 2.0',
        'bz2file',
        'requests',
        'smart_open'
    ]
)
