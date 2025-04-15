from setuptools import setup, find_packages

setup(
    name="motor-ingesta",
    version="0.1.0",
    author="Andres Felipe Forero Correa",
    author_email="andfor01@ucm.es",
    description="Motor de ingesta para el curso de Spark",
    long_description="Motor de ingesta para el curso de Spark",
    long_description_content_type="text/markdown",
    url="https://github.com/afforeroc",
    python_requires=">=3.11",
    packages=find_packages(),
    package_data={"motor_ingesta": ["resources/*.csv"]}
)
