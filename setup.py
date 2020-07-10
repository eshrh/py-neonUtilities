import setuptools
import os
with open('README.md') as f:
    long_description = f.read()

setuptools.setup(
      name='py-neonUtilities',
      version='0.4.0',
      description='A native python wrapper for the NEON ecological database api',
      long_description=long_description,
      long_description_content_type="text/markdown",
      packages=setuptools.find_packages(),
      python_requires='>=3.6',
      author='esrh',
      author_email='esrh@netc.eu',
      keywords='NEON data ecology api',
      url='https://gitlab.com/esrh/py-neonutils')
