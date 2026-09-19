package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

// Self-signed cert helpers

func generateSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("key pair: %v", err)
	}
	return cert
}

func makeTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	cert := generateSelfSignedCert(t)
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

// Other Helpers

func startDropper(t *testing.T) (*httpDropper, string) {
	t.Helper()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hd := &httpDropper{listener: inner, tlsCfg: makeTLSConfig(t)}
	return hd, inner.Addr().String()
}

func acceptOne(hd *httpDropper) <-chan error {
	ch := make(chan error, 1)
	go func() {
		conn, err := hd.Accept()
		if err != nil {
			ch <- err
			return
		}
		_ = conn.(*tls.Conn).Handshake()
		conn.Close()
		ch <- nil
	}()
	return ch
}

// Tests

func TestHTTPDropper(t *testing.T) {
	t.Run("TLSClientHandshakes", func(t *testing.T) {
		hd, addr := startDropper(t)
		defer hd.Close()

		serverDone := acceptOne(hd)

		conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Fatalf("tls.Dial: %v", err)
		}
		conn.Close()

		select {
		case err := <-serverDone:
			if err != nil {
				t.Fatalf("server accept error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for server accept")
		}
	})

	t.Run("PlainHTTPIsDropped", func(t *testing.T) {
		hd, addr := startDropper(t)
		defer hd.Close()

		serverDone := acceptOne(hd)

		raw, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer raw.Close()

		_, _ = raw.Write([]byte("GET / HTTP/1.0\r\n\r\n"))

		select {
		case <-serverDone:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout: server blocked on plain-HTTP connection")
		}

		raw.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 64)
		n, readErr := raw.Read(buf)
		if n > 0 {
			t.Errorf("expected no data from dropped connection, got %d bytes: %q", n, buf[:n])
		}
		if readErr == nil {
			t.Error("expected error reading from dropped connection, got nil")
		}
	})

	t.Run("EmptyConnectionIsDropped", func(t *testing.T) {
		hd, addr := startDropper(t)
		defer hd.Close()

		serverDone := acceptOne(hd)

		raw, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		raw.Close()

		select {
		case <-serverDone:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout: server blocked on empty connection")
		}
	})

	t.Run("CloseUnblocksAccept", func(t *testing.T) {
		hd, _ := startDropper(t)

		errCh := make(chan error, 1)
		go func() {
			_, err := hd.Accept()
			errCh <- err
		}()

		hd.Close()

		select {
		case err := <-errCh:
			if err == nil {
				t.Error("expected error after Close, got nil")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for Accept to return after Close")
		}
	})

	t.Run("AddrDelegates", func(t *testing.T) {
		hd, addr := startDropper(t)
		defer hd.Close()

		if hd.Addr().String() != addr {
			t.Errorf("Addr() = %q, want %q", hd.Addr().String(), addr)
		}
	})

	t.Run("PeekedConnReadIsIdempotentAfterDrop", func(t *testing.T) {
		server, client := net.Pipe()
		defer server.Close()

		go func() {
			client.Write([]byte("GET")) // 'G' = 0x47, not 0x16
			client.Close()
		}()

		pc := &peekedConn{Conn: server}
		buf := make([]byte, 16)

		for i := range 3 {
			n, err := pc.Read(buf)
			if n != 0 || err != io.EOF {
				t.Errorf("Read #%d: got (%d, %v), want (0, io.EOF)", i+1, n, err)
			}
		}
	})
}
