from setuptools import setup

import sys
name = 'Leaf'
version = '0.1'
setup(
    name = name,
    version = version,
    author = "Thomas Huang",
    description = "Leaderboard System",
    license = "MIT",
    keywords = "Leaderboard",
    url='https://github.com/thomashuang/Leaf',
    long_description=open('README.md').read(),
    packages = ['leaf', 'leaf.model', 'leaf.thing'],
    classifiers=(
        "Development Status :: 5 - Production/Stable",
        "License :: MIT",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet :: Leaderboard"
        )
    )