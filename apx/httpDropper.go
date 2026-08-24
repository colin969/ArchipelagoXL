package main

import (
	"crypto/tls"
	"io"
	"net"
	"sync"
)

// Special listener to mimic Python's transport.abort() behaviour for non-TLS traffic
type httpDropper struct {
	listener net.Listener
	tlsCfg   *tls.Config
}

func (hd *httpDropper) Accept() (net.Conn, error) {
	// Just accept the connection, but we'll wrap it
	conn, err := hd.listener.Accept()
	if err != nil {
		return nil, err
	}
	peeked := &peekedConn{Conn: conn}
	return tls.Server(peeked, hd.tlsCfg), nil
}

func (hd *httpDropper) Close() error   { return hd.listener.Close() }
func (hd *httpDropper) Addr() net.Addr { return hd.listener.Addr() }

type peekedConn struct {
	net.Conn
	once   sync.Once
	r      io.Reader
	dropMe bool
}

func (c *peekedConn) Read(b []byte) (int, error) {
	// Only run on first read, get first byte
	c.once.Do(func() {
		buf := make([]byte, 1)
		_, err := io.ReadFull(c.Conn, buf)
		// If not TLS byte, mark to drop
		if err != nil || buf[0] != 0x16 {
			c.dropMe = true
			return
		}
		// Join byte back on
		c.r = io.MultiReader(bytesReader(buf), c.Conn)
	})

	if c.dropMe {
		if tc, ok := c.Conn.(*net.TCPConn); ok {
			tc.SetLinger(0) // RST immediately, discard buffers — matches transport.abort()
		}
		c.Conn.Close()
		return 0, io.EOF
	}

	return c.r.Read(b)
}

func bytesReader(b []byte) io.Reader {
	return &singleByteReader{b: b}
}

type singleByteReader struct{ b []byte }

func (r *singleByteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
