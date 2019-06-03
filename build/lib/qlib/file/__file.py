import os
import re
import sys
import time
import zipfile
from hashlib import md5
from termcolor import cprint, colored
from base64 import b64encode, b64decode
from io import BytesIO

from qlib.log import LogControl

LogControl.LOG_LEVEL = LogControl.WRN

j = os.path.join

def to_save(line,ty,root_dir):
    if not os.path.exists(os.path.join(root_dir,ty)):
        with open(os.path.join(root_dir,ty), "w") as fp:
            pass

    with open(os.path.join(root_dir,ty), "a+") as fp:
        print(line, file=fp)

def file_search(info, fs):
    for f in fs:
        cprint("--> file: %15s" % colored(f,attrs=['bold']), 'yellow', file=sys.stderr)
        with open(f) as fp:
            dic = {}
            for line in fp:
                l = line.strip()
                if re.search(re.compile('(%s)' % info), l):
                    yield l

def call_vim(tmp_file="/tmp/add.log.tmp.log"):
    from fabric.api import output
    from fabric.api import local
    output.running = False
    local("vim %s" % tmp_file)
    time.sleep(0.5)
    with open(tmp_file) as fp:
        text = fp.read()
        return text


def fzip(src, dst, direction='zip'):
    """
    @direction: 
        'zip/unzip'  to decide pack file or unpack file.
        default: 'zip'

    """

    try:
        if direction == 'zip':

            with  zipfile.ZipFile(src,'w', zipfile.ZIP_BZIP2)  as f:
                if os.path.isdir(dst):
                    for dirpath, dirnames, filenames in os.walk(dst):
                        for filename in filenames:  
                            f.write(os.path.join(dirpath,filename))
                elif os.path.isfile(dst):
                    f.write(dst)
                else:
                    raise OSError("file not exists! ", dst)

        elif direction == 'unzip':
            with zipfile.ZipFile(src,'r')  as zfile:
                for filename in zfile.namelist():
                    _p = j(dst, filename)
                    # LogControl.err(_p)
                    _d = '/'.join(_p.split("/")[:-1])
                    if not os.path.exists(_d):
                        os.makedirs(_d)
                    data = zfile.read(filename)
                    # LogControl.wrn(_p)
                    file = open(_p, 'w+b')  
                    file.write(data)  
        else:
            print("no such direction")
            return False
        return True
    except Exception as e:
        LogControl.err(e)
        return False


def zip_64(fpath):
    """
    zip a file then encode base64. return it.
    """
    tmp_zip = "/tmp/tmp." + md5(fpath.encode("utf8")).hexdigest()[:10] + ".zip"
    if os.path.exists(tmp_zip):
        with open(tmp_zip, "w") as f:
            pass

    fzip(tmp_zip , fpath)
    with open(tmp_zip, 'rb') as fp:
        return b64encode(fp.read())


def unzip_64(data, fpath, override=False):
    """
    decode b64-zip file ,then write data to fpath.
    """
    if not os.path.isdir(fpath):
        try:
            os.makedirs(fpath)
        except OSError as e:
            LogControl.err(e)
            return False

    zdata = None
    try:
        zdata = b64decode(data)
    except Exception as e:
        LogControl.err("not b64 format")
        return False


    bfp = BytesIO(zdata)
    try:
        with zipfile.ZipFile(bfp, 'r') as zfp:
            for filename in zfp.namelist():
                _p = j(fpath, filename)
                # LogControl.err(_p)
                _d = '/'.join(_p.split("/")[:-1])

                # mkdir -p 
                if not os.path.exists(_d):
                    os.makedirs(_d)

                # read real data from zip archivement.
                data = zfp.read(filename)

                # write data.
                with open(_p, 'w+b') as wbfp:
                    wbfp.write(data)
    except Exception as e:
        LogControl.err(e)
        return False

    return True

def file264(fpath):
    if os.path.isfile(fpath):
        with open(fpath, 'rb') as fp:
            return b64encode(fp.read())

def b642file(b_str):
    return b64decode(b_str)
