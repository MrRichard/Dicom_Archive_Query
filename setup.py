from setuptools import setup, find_packages

setup(
    name='ditag',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click',
        'pydicom',
        'pynetdicom',
    ],
    entry_points={
        'console_scripts': [
            'ditag = ditag.cli:cli',
        ],
    },
)
