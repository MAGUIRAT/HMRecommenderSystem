import setuptools

setuptools.setup(
    name="HMRecommenderSystem",
    version="0.0.1",
    author="Mohamed Ali Guirat",
    author_email="dali2.guirat@gmail.com",
    description="No description",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "."},
    packages=setuptools.find_packages(where="."),
    python_requires=">=3.8")
