from setuptools import setup, find_packages

setup(
    name='patstat_importer',
    version='0.1',
    packages=find_packages(),
    description='A Python package for importing PATSTAT database tables and converting them to Parquet format.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Stefano Breschi',
    author_email='stefano.breschi@unibocconi.it',
    url='https://github.com/sbreschi/patstat-importer',
    license='MIT',
    install_requires=[
        'pandas',
        'pyarrow',
        'numpy',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ],
    python_requires='>=3.7',
    include_package_data=True,
    zip_safe=False
)