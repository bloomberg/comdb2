#!/usr/bin/env python3

import socket
import sys
import unittest
import threading

numports=1000
startport = 19000
port=5105
unix_path='/tmp/portmux.socket'

class BufferedSocketReader(socket.socket):
    def __init__(self, family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, fileno=None):
        super().__init__(family, type, proto, fileno)
        self.file = self.makefile(mode='r')
    def close(self):
        self.file.close()
        super().close()
    def get_response(self):
        return self.file.readline().rstrip('\n')

def send(c, req):
    return c.send(req.encode())

def pmux_tcp_socket():
    s = BufferedSocketReader()
    s.connect(('localhost', port))
    return s

def pmux_unix_socket():
    s = BufferedSocketReader(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(unix_path)
    return s

def rte_tcp(svc):
    s = pmux_tcp_socket()
    send(s, 'rte ' + svc + '\n')
    return s

def rte_unix(svc):
    s = pmux_unix_socket()
    send(s, 'rte ' + svc + '\n')
    return s

def get_tcp(svc):
    s = pmux_tcp_socket()
    send(s, 'get ' + svc + '\n')
    return s

def get_unix(svc):
    s = pmux_unix_socket()
    send(s, 'get ' + svc + '\n')
    return s

def recv_socket(s):
    data, ancdata, msg_flags, address = s.recvmsg(4, socket.CMSG_LEN(sys.getsizeof(int())))
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if (cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS):
            return data.decode(),  BufferedSocketReader(fileno = int.from_bytes(cmsg_data, sys.byteorder))

class Pmux(unittest.TestCase):

    def do_route(self, name):
        name = 'aks'+name
        namelen = len(name)
        s = pmux_unix_socket()
        self.assertEqual(send(s, 'reg ' + name + '\n'), 5 + namelen)
        s.get_response()
        for i in range(10240):
            c = pmux_tcp_socket()
            self.assertEqual(send(c, 'rte ' + name + '\n'), 5 + namelen)
            p, r = recv_socket(s)
            self.assertEqual(p, 'pmux')
            self.assertIsNotNone(r)
            self.assertEqual(send(r, '0\n'), 2)
            self.assertEqual(c.get_response(), '0')
            self.assertEqual(send(c, 'Hello,World!\n'), 13)
            self.assertEqual(r.get_response(), 'Hello,World!')
            self.assertIsNone(r.close())
            self.assertIsNone(c.close())
        self.assertEqual(send(s, 'del ' + name + '\n'), 5 + namelen)
        self.assertEqual(s.get_response(), '0')
        self.assertIsNone(s.close())

    def test_skip_reading_reply(self):
        sockets = [get_tcp('foo'), get_unix('foo'), rte_tcp('foo'), rte_unix('foo')]
        for s in sockets:
            self.assertIsNone(s.close())

    def test_svc_not_found(self):
        sockets = [get_tcp('foo'), get_unix('foo'), rte_tcp('foo'), rte_unix('foo')]
        for s in sockets:
            self.assertIsNotNone(s)
            self.assertEqual(s.get_response(), '-1')
            s.close()

    def test_slow_writer(self):
        slow = pmux_tcp_socket()
        fast = pmux_tcp_socket()
        self.assertEqual(send(slow, 's'), 1)
        self.assertEqual(send(fast, 'stat\n'), 5)
        self.assertEqual(fast.get_response(), 'free ports: ' + str(numports))
        self.assertEqual(send(slow, 'ta'), 2)
        self.assertEqual(send(fast, 'reg abc\n'), 8)
        self.assertEqual(fast.get_response(), str(startport))
        self.assertEqual(send(slow, 't\n'), 2)
        self.assertEqual(slow.get_response(), 'free ports: 999')
        self.assertEqual(send(fast, 'del abc\n'), 8)
        self.assertEqual(fast.get_response(), '0')
        fast.close()
        slow.close()

    def test_tcp(self):
        s = pmux_tcp_socket()
        self.assertEqual(send(s, 'reg aks\n'), 8)
        self.assertEqual(s.get_response(), str(startport))
        self.assertIsNone(s.close())
        c = pmux_tcp_socket()
        self.assertEqual(send(c, 'get aks\n'), 8)
        self.assertEqual(c.get_response(), str(startport))
        self.assertEqual(send(c, 'get /echo aks\n'), 14)
        self.assertEqual(c.get_response(), str(startport) + ' aks')
        self.assertEqual(send(c, 'del aks\n'), 8)
        self.assertEqual(c.get_response(), '0')
        self.assertIsNone(c.close())

    def test_parallel_route(self):
        thds = []
        for i in range(16):
            thds.append(threading.Thread(target=self.do_route, args=(str(i),)))
        for t in thds:
            t.start()
        for t in thds:
            t.join()

    def test_route(self):
        self.do_route('')

    def test_already_active(self):
        xyz = pmux_unix_socket()
        self.assertEqual(send(xyz, 'reg xyz\n'), 8)
        self.assertEqual(xyz.get_response(), str(startport))
        usockets = []
        for i in range(100):
            u = pmux_unix_socket()
            usockets.append(u)
            self.assertEqual(send(u, 'reg xyz\n'), 8)
            self.assertEqual(u.get_response(), '-1 service already active')
        for u in usockets:
            self.assertIsNone(u.close())
        self.assertEqual(send(xyz, 'del xyz\n'), 8)
        self.assertEqual(xyz.get_response(), '0')
        self.assertIsNone(xyz.close())

    def test_empty_cmd(self):
        t = pmux_tcp_socket()
        self.assertEqual(send(t, '\n'), 1)
        self.assertEqual(t.get_response(), '-1 missing command')
        self.assertIsNone(t.close())

    def test_large_cmd(self):
        num=10240
        t = pmux_tcp_socket()
        self.assertEqual(send(t, 'f' * num), num)
        with self.assertRaises(ConnectionResetError):
            t.get_response()
        with self.assertRaises(BrokenPipeError):
            send(t, 'f' * num)
        self.assertIsNone(t.close())

    def test_req_many(self):
        num = 1000
        t = pmux_tcp_socket()
        self.assertEqual(send(t, 'reg many\n'), 9)
        self.assertEqual(t.get_response(), str(startport))
        many = pmux_tcp_socket()
        self.assertEqual(send(many, 'get many\n' * num), 9 * num)
        portlen = len(str(startport)) + 1
        resp = []
        while len(resp) != num:
            resp.append(many.recv(portlen).decode().split('\n')[0])
        self.assertEqual(len(resp), num)
        for r in resp:
            self.assertEqual(r, str(startport))
        self.assertIsNone(many.close())
        self.assertEqual(send(t, 'del many\n'), 9)
        self.assertEqual(t.get_response(), '0')
        self.assertIsNone(t.close())

    def test_max_ports(self):
        sockets = []
        for i in range(numports // 2): # // is floor division
            t = pmux_tcp_socket()
            sockets.append(t)
            u = pmux_unix_socket()
            sockets.append(u)
            cmd = 'reg t' + str(i) + ' \n'
            self.assertEqual(send(t, cmd), len(cmd))
            cmd = 'reg u' + str(i) + ' \n'
            self.assertEqual(send(u, cmd), len(cmd))
            p = startport + (i * 2)
            self.assertEqual('t' + str(i) + t.get_response(), 't' + str(i) + str(p))
            self.assertEqual('u' + str(i) + u.get_response(), 'u' + str(i) + str(p + 1))
        #should fail to get more ports on tcp and unix
        s = pmux_tcp_socket()
        sockets.append(s)
        self.assertEqual(send(s, 'reg f\n'), 6)
        self.assertEqual(s.get_response(), '-1')
        s = pmux_unix_socket()
        sockets.append(s)
        self.assertEqual(send(s, 'reg f\n'), 6)
        self.assertEqual(s.get_response(), '-1')
        for i in range(numports // 2): # // is floor division
            cmd = 'del t' + str(i) + '\ndel u' + str(i) + '\n'
            self.assertEqual(send(s, cmd), len(cmd))
            self.assertEqual(s.get_response(), '0')
            self.assertEqual(s.get_response(), '0')
        for s in sockets:
            self.assertIsNone(s.close())

    def test_open_close(self):
        for i in range(100):
            s = pmux_unix_socket()
            self.assertEqual(send(s, 'reg openclose\n'), 14)
            self.assertEqual(s.get_response(), str(startport))
            self.assertIsNone(s.close())
        s = pmux_tcp_socket()
        self.assertEqual(send(s, 'del openclose\n'), 14)
        self.assertEqual(s.get_response(), '0')
        s.close()

    #def test_foo(self):
    #    name = 'foo'
    #    namelen = len(name)
    #    s = pmux_tcp_socket()
    #    self.assertEqual(send(s, 'reg ' + name + '\n'), 5 + namelen)
    #    s.get_response()
    #    c = pmux_tcp_socket()
    #    self.assertEqual(send(c, 'rte ' + name + '\n'), 5 + namelen)
    #    p, r = recv_socket(s)
    #    self.assertEqual(p, 'pmux')
    #    self.assertIsNotNone(r)
    #    self.assertEqual(send(r, '0\n'), 2)
    #    self.assertEqual(c.get_response(), '0')
    #    self.assertEqual(send(c, 'Hello,World!\n'), 13)
    #    self.assertEqual(r.get_response(), 'Hello,World!')
    #    self.assertIsNone(r.close())
    #    self.assertIsNone(c.close())
    #    self.assertEqual(send(s, 'del ' + name + '\n'), 5 + namelen)
    #    self.assertEqual(s.get_response(), '0')
    #    self.assertIsNone(s.close())

if __name__ == '__main__':
    unittest.main()
