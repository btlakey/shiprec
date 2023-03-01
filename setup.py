from setuptools import setup, find_packages

setup(
    name='shiprec',
    version='0.1',
    description='graph neural network book recommendations, built on scraped Goodreads data',
    author='Brian Lakey',
    author_email='brianlakey@gmail.com',
    license="MPL-2.0",
    packages=find_packages(),
    install_requires=[
        'Scrapy==2.8.0',
        'pyarrow==11.0.0',
        'toolz==0.12.0'
    ]
)
