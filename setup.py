import setuptools
REQUIRED_PACKAGES = [
    "apache_beam[gcp]",
]

PACKAGE_NAME = 'my_package'
PACKAGE_VERSION = '0.0.1'
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Installing packs',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)