from setuptools import find_packages, setup

setup(
    name="dagster_reddit",
    packages=find_packages(exclude=["dagster_reddit_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-postgres",
        "dagster-aws",
        "pandas",
        "telebot"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
