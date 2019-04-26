from fabric.api import *
from qlib.io import GeneratorApi
from qlib.log import L, LogControl
from termcolor import colored
import os
from base64 import b64decode


default_ss_file = 'ewogICAgInNlcnZlciI6IjAuMC4wLjAiLAogICAgInBvcnRfcGFzc3dvcmQiOiB7CiAgICAgICAgIjEzMDAxIjogInRoZWZvb2xpc2gxIiwKICAgICAgICAiMTMwMDIiOiAidGhlZm9vbGlzaDIiLAogICAgICAgICIxMzAwMyI6ICJ0aGVmb29saXNoMyIsCiAgICAgICAgIjEzMDA0IjogInRoZWZvb2xpc2g0IiwKICAgICAgICAiMTMwMDUiOiAidGhlZm9vbGlzaDUiLAogICAgICAgICIxMzAwNiI6ICJ0aGVmb29saXNoNiIsCiAgICAgICAgIjEzMDA3IjogInRoZWZvb2xpc2g3IiwKICAgICAgICAiMTMwMDgiOiAidGhlZm9vbGlzaDgiLAogICAgICAgICIxMzAwOSI6ICJ0aGVmb29saXNoOSIsCiAgICAgICAgIjEzMDEwIjogInRoZWZvb2xpc2gxMCIsCiAgICAgICAgIjEzMDExIjogInRoZWZvb2xpc2gxMSIsCiAgICAgICAgIjEzMDEyIjogInRoZWZvb2xpc2gxMiIsCiAgICAgICAgIjEzMDEzIjogInRoZWZvb2xpc2gxMyIKICAgIH0sCiAgICAid29ya2VycyI6IDE1LAogICAgIm1ldGhvZCI6ImFlcy0yNTYtY2ZiIgp9Cg=='

@task
def list():
    for i in env.roledefs:
        L(i, env.roledefs[i][0], color='blue')

def os_test():
    if test("apt-get"):
        return "apt"
    else:
        return "yum"


def test(so):
    try:
        res = run(so + " -v", quiet=True)
        if "command not found" in res:
            return False
    except Exception as e:
        return False
    return True

def ex_cmd(cmd):
    L(colored("[-]",'green'),cmd, color='blue')
    res = run(cmd, quiet=True)
    L('[T]',env.host,colored('-'* (LogControl.SIZE[1] -24),'red'))
    L(res,color='green')



def keysave():
    k = os.path.join(os.getenv("HOME"), ".ssh/id_rsa.pub")
    if os.path.exists(k):
        pass
    else:
        local("ssh-keygen -t rsa -P \"\"")

    with open(k) as fp:
        ww = fp.read()
        L(ww)
        ex_cmd("mkdir -p ~/.ssh && echo \"%s\" >> ~/.ssh/authorized_keys " % ww)


def kill(cmd):
    s = 'ps aux | grep %s | egrep -v "(grep|egrep)" | awk \'{print $2}\'  | xargs kill -9' % cmd
    run(s, quiet=True)


@task
@parallel
def shadowsocks(start=False, stop=False):
    if stop:
        kill("ssserver")

    installer = "apt-get"
    if not start:
        installer = os_test()
        if installer == "yum":
            ex_cmd("yum install -y epel-release")

    if not test("ssserver"):
        ex_cmd(installer + " install -y shadowsocks-libev")
        if not test("pip"):
            ex_cmd(installer + " install -y python-pip")
            ex_cmd(installer + " install -y python-wheel python-setuptools")
            

        ex_cmd("pip install shadowsocks")
    else:

        
        with open("/tmp/default.json", "wb") as fp:
            fp.write(b64decode(default_ss_file))

        put("/tmp/default.json", "/tmp/")

        if start:
            ex_cmd("ssserver -d start -c /tmp/default.json")
            L("Ss - start!")





@task
@parallel
def go():
    output.running = False
    local("ssh root@"+ env.host)

@task
@parallel
def build_msf():
    imsg = run("docker images | grep msf ", quiet=True)
    if not imsg:
        ex_cmd("docker pull phocean/msf", quiet=True)
    fs= run("docker ps -a | grep msf ", quiet=True).strip()
    if not fs:
        run("docker run --rm -i -t -p 9990-9999:9990-9999 -v /root/.msf4:/root/.msf4 -v /tmp/msf:/tmp/data --name msf phocean/msf")
    else:
        output.running= False
        local("ssh root@" + env.host  + "  `docker attach msf`")


@task
@parallel
def up(f):
    put(f, "/tmp/")

@task
@parallel
def breakOs():
    ex_cmd("docker ps -a | awk '{print $1 }' | xargs docker rm -f ") 
    ex_cmd("docker images  | awk '{print $3 }' | xargs docker rmi -f ") 
    ex_cmd(" rm -rf /var/log ")
    ex_cmd(" rm  -rf ~/")
    ex_cmd(" rm -rf /tmp")
    ex_cmd(" rm -rf /opt")
    ex_cmd(" rm -rf /usr")
    ex_cmd(" rm -rf /home")
    ex_cmd(" rm -rf /bin")
    ex_cmd(" rm -rf /etc")
    ex_cmd(" rm -rf /srv")



@task
@parallel
def ex(cmd):
    output.running = False
    ex_cmd(cmd)
