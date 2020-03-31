try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

readme = open('README.md').read()

setup(
    long_description=readme,
    long_description_content_type='text/markdown',
    name='blobby',
    version='0.0.0',
    description='A library to provide a simple abstraction of an object store.',
    python_requires='~=3.7',
    packages=['blobby'],
    package_dir={"": "."},
    package_data={},
    install_requires=['boto3', 'chardet', 'urllib3'],
    dependency_links=[
        'git+https://github.com/boto/botostubs.git#egg=botocore-stubs-0.0.0'
    ],
    extras_require={
        "dev": [
            "boto3-stubs[s3]",
            "botocore-stubs @ git+https://github.com/boto/botostubs.git#egg=botocore-stubs-0.0.1&subdirectory=botocore-stubs",
            "coveralls",
            "flake8",
            "mypy",
            "pytest",
            "pytest-cov"
        ]
    },
    test_suite='test',
)
