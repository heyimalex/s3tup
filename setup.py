from setuptools import setup

setup(name='s3tup',
      version='0.0.5',
      description='Declarative configuration management tool for amazon s3',
      url='http://github.com/HeyImAlex/s3tup',
      author='Alex Guerra',
      author_email='alex@heyimalex.com',
      license='MIT',
      packages=['s3tup'],
      install_requires=[
          'requests',
          'argparse',
          'pyyaml',
          'beautifulsoup4',
      ],
      entry_points = {
        'console_scripts': ['s3tup=s3tup.cli:main'],
      },
      zip_safe=False)