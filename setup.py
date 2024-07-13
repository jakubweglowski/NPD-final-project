# I have my python project in github repo under link https://github.com/jakubweglowski/NPD-final-project. I need to deliver it as a package that can be installed with the pip
# command (in the form of pip install ./path/to/package). How can I do it?
from setuptools import setup, find_packages

setup(
    name='npd-final-project',
    version='1.0',
    packages=find_packages(),
    install_requires=['pandas',
                      'wbgapi',
                      'datetime',
                      'numpy',
                      'langdetect',
                      'unidecode',
                      'snakeviz'],
    author='Jakub Węgłowski',
    author_email='jw430620@students.mimuw.edu.pl'
)