import os
import sys
from  os.path import join as j

from qlib.c import include, C, c, c_long, c_short,  c_ushort, c_char, c_char_p, c_int, c_ubyte, Structure, POINTER
from qlib.c import structure, c_restype

class LibStructure:
    """
    """

    @structure(1024)
    class py_ip_pack(C):
        _raw = """
    typedef struct ip_packet{
        long ip_len;
        long payload_len;
        char src[16] ;
        char dst[16] ;
        u_char protocol;
        u_char payload[1500];
    } py_ip_pack; 
    """
        pass


    @structure(1024)
    class py_tcp(C):
        """docstring for Tcp"""
        _raw = """
typedef struct tcp_packet{
    long len;
    long payload_len;
    unsigned short sport;
    unsigned short dport;
    unsigned int seq;
    unsigned int ack;
    unsigned int off;
    u_char flags;
    unsigned short checksum;
    unsigned short windows;
    u_char payload[1500];
} py_tcp;

        """
        pass


    @structure(1024)
    class py_udp(C):
        _raw = """

    typedef struct udp_packet{
        unsigned short sport;
        unsigned short dport;
        unsigned short ulen;
        unsigned short checksum;
        u_char * payload;
    }py_udp;
        """

# class Packet(C):
#     _fields_ = [
#         ()
#     ]


# this decorator will auto load lib
qlib_c_lib_path = j(os.getenv("HOME"), ".Q/clib/")


@include(qlib_c_lib_path, LibStructure)
class libsniff:
    """
    黑魔法的应用例子  , 三个函数 @structure, @include , @c
    
    structure :int ， 这个参数是指明 u_char * 长度上限的。。。不加容易崩溃，当常量吧
    创建一个c 中的同名类，继承 C， 然后直接把 结构体粘贴过来 ps: 必须是 gnu 标准的
    放到 _raw 变量里 。好了，c结构体转 py完成

    如下：
    @structure(1024)
    class py_udp(C):
        _raw = \"\"\"
    typedef struct udp_packet{
        unsigned short sport;
        unsigned short dport;
        unsigned short ulen;
        unsigned short checksum;
        u_char * payload;
    }py_udp;
    \"\"\"

    include str: lib path , obj: a place kan get structures
    创建一个和 so lib库同名的类 ， 第一个参数是 c 库的目录 ，（因为我偷懒， 必须把头文件和 so文件都放在这)
    第二个 变量 随便一个 obj 都行 ，随便放，只要 包含需要用到的 结构体类 ，结构体类的创建就上上面那个，非常傻瓜的方式

    
    c
    这个函数很黑，
    转换py的基础结构到c的结构，这里的基础结构，指凡是 ctypes能对应到 c的py结构都行
    比如 str -> c_char_array , int -> c_int , 
    复合结构也能自动转换 [[2,3], [3,4]] -> c_int_array_2_int_array_2 

    """

    @c
    def __init__(self, dev):
        libsniff.init_pcap(dev)

    @c
    def ip_pack(self, filter):
        # self.next_ip_pack.restype = POINTER(Ip)
        return self.next_ip_pack(filter)

    @c
    def tcp(self,cmd):
        ip = self.ip_pack(cmd)
        if ip.contents.protocol == 6:
            return self.to_tcp(ip)
        else:
            return self.tcp(cmd)

    @c
    def udp(self, cmd):
        ip = self.ip_pack(cmd)
        if ip.contents.protocol == 17:
            return self.to_udp(ip)
        elif ip.contents.protocol == 1:
            print("icmp:",ip.src,'->',ip.dst)
        else:
            return self.udp(cmd)

    @c
    def one(self, cmd):
        ip = self.ip_pack(cmd)
        if ip.contents.protocol == 6:
            t = self.to_tcp(ip)
            t.src = ip.contents.src.decode("utf8","ignore").strip()
            t.dst = ip.contents.dst.decode("utf8","ignore").strip()
            return t
        elif ip.contents.protocol == 17:
            u = self.to_udp(ip)
            u.src = ip.contents.src.decode("utf8","ignore").strip()
            u.dst = ip.contents.dst.decode("utf8","ignore").strip()
            return u
        else:
            ip.src = ip.contents.src.decode("utf8","ignore").strip()
            ip.dst = ip.contents.dst.decode("utf8","ignore").strip()
            return ip

    @c
    def mul(self, cmd):
        count = 0
        try:
            while True:
                count += 1
                yield self.one(cmd)
        
        except KeyboardInterrupt:
            print(count)


    def payload(self, cmd, pre_func):
        for p in self.mul(cmd):
            if p.contents.payload_len == 0:
                continue
            pay = pre_func((p.src, p.dst), p.contents.payload[:p.contents.payload_len])
            yield pay


