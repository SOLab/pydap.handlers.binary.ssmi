from setuptools import setup, find_packages
import sys, os


version = '0.1'

install_requires = [
    # List your project dependencies here.
    # For more details, see:
    # http://packages.python.org/distribute/setuptools.html#declaring-dependencies
    'Pydap',
    'ConfigObj',
    'pupynere',
    'Numpy',
]


setup(name='pydap.handlers.binary.ssmi',
    version=version,
    description="Handler for that allows Pydap to serve binary SSMI data",
    long_description="",
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords='netcdf opendap dods dap science meteorology oceanography',
    author='Ilya Bolkhovsky',
    author_email='ilya@rshu.ru',
    url='https://github.com/bolhovsky/pydap.handlers.binary.ssmi',
    license='MIT',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    namespace_packages = ['pydap', 'pydap.handlers'],
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points="""
        [pydap.handler]
        ssmi = pydap.handlers.binary.ssmi:BinarySsmiHandler
    """,
)
