import os
import glob
from setuptools import setup, find_packages

install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

setup(name='gs-chunked-io',
      version='0.1.1',
      description='Streaming read/writes to Google Storage blobs with ascynchronous buffering.',
      url='https://github.com/xbrianh/gs-chunked-io.git',
      author='Brian Hannafious',
      author_email='bhannafi@ucsc.edu',
      license='MIT',
      packages=find_packages(exclude=['tests']),
      scripts=glob.glob('scripts/*'),
      zip_safe=False,
      install_requires=install_requires,
      platforms=['MacOS X', 'Posix'],
      test_suite='test',
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3.7'
      ]
      )
