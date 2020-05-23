import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="threadmanager",
    version="0.0.5",
    author="Brandon M. Pace",
    author_email="brandonmpace@gmail.com",
    description="A thread manager for Python programs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="thread state manager",
    license="GNU Lesser General Public License v3 or later",
    install_requires=["prettytable>=0.7.2"],
    platforms=['any'],
    python_requires=">=3.6.5",
    url="https://github.com/brandonmpace/threadmanager",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3"
    ]
)
