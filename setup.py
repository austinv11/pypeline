from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = [l for l in fh if l]

setup(
    name='data-pypeline',
    version='0.1.4',
    packages=find_packages(exclude=('tests',)),
    url='https://github.com/austinv11/pypeline',
    project_urls={
        'Bug Reports': 'https://github.com/austinv11/pypeline/issues',
        # 'Funding': 'https://donate.pypi.org',
        # 'Say Thanks!': 'http://saythanks.io/to/example',
        'Source': 'https://github.com/austinv11/pypeline/',
    },
    license='Apache License 2.0',
    author='austinv11',
    author_email='austinv11@gmail.com',
    description='Pypeline is a data pipeline builder library.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: Apache Software License",
        "Framework :: AsyncIO",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Utilities",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    keywords='data pipeline pypeline',
    install_requires=requirements,
)
