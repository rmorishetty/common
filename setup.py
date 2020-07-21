import os
import setuptools

VERSION = "0.0.1"
NAME = "ade_utils"
DESCRIPTION = "DESCRIPTION utils"
AUTHOR = " Team"
BUCKET_NAME = os.getenv("BUCKET_NAME")
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
ADE_UTILS_DIR = os.path.join(CURRENT_DIR, "xxx")


def main():

    f = open(ADE_UTILS_DIR+'/constants.py', 'w')
    output = "BUCKET_NAME = '%s'" % BUCKET_NAME
    f.write(output)
    f.close()
    setuptools.setup(
        name=NAME,
        version=VERSION,
        author=AUTHOR,
        description=DESCRIPTION,
        long_description="Common Utils for Data Engineering AWS Work",
        long_description_content_type="text/markdown",
        packages=setuptools.find_packages(),
        classifiers=[]
    )
    os.system('cp ./dist/*.whl ./packages')
    os.system('zip -j ./packages/xxx.zip `find ./dist -name "*.whl"`')


if __name__ == '__main__':
    main()
