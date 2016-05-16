from setuptools import setup, find_packages

setup(name="phlock2",
    packages=find_packages(exclude=['tests']),
    install_requires=['boto'],
    package_data={'phlock2': ['*.R']},
    entry_points={
        'console_scripts': [
            'phlock2=phlock2.cmds:main',
            'phlock2-submit=phlock2.cmds:submit_main',
            'phlock2-bake=phlock2.push_image:bake'
        ],
    }
)