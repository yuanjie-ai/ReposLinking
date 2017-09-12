# coding: utf-8
__title__ = 'PythonZip'
__author__ = 'JieYuan'
__mtime__ = '2017/9/9'

import zipfile
class PythonZip(object):
    """doc"""
    def __init__(self, ):
        """doc"""
        pass
    @staticmethod
    def zip(targetname, filename, path='./'):
        f = zipfile.ZipFile(targetname, 'w', zipfile.ZIP_DEFLATED)
        f.write(filename, path)
        f.close()

    @staticmethod
    def unzip(zipfilePath, path="./"):
        f = zipfile.ZipFile(zipfilePath, 'r')
        for file in f.namelist():
            f.extract(file, path)

