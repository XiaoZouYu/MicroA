from setuptools import find_packages, setup


setup(
    name="mia",
    version="0.0.1",
    description="一个基于 `asyncio` 简易的微服务框架",
    long_description="",
    author="XiaoZouYu",
    url="",
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "aio-pika>=9.3.0",
        "uvloop>=0.16",
        "pyyaml>=6.0.0",
    ],
    extras_require={},
    entry_points={
        'console_scripts': [
            'mia=mia.cli.main:main',
        ],
        'pytest11': [
            'pytest_mia=mia.testing.pytest'
        ]
    },
    zip_safe=True,
)
