package kafka

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kafka-stress-test/internal/config"
)

func generateSelfSignedCert(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	// Generate private key
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privKey)})

	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	if err := os.WriteFile(caPath, certPEM, 0644); err != nil {
		t.Fatalf("failed to write ca.pem: %v", err)
	}
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		t.Fatalf("failed to write cert.pem: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key.pem: %v", err)
	}

	return caPath, certPath, keyPath
}

func TestBuildTLSConfig(t *testing.T) {
	tempDir := t.TempDir()
	caPath, certPath, keyPath := generateSelfSignedCert(t, tempDir)

	cfg := config.DefaultConfig()
	cfg.TLSEnabled = true
	cfg.TLSCAFile = caPath
	cfg.TLSCertFile = certPath
	cfg.TLSKeyFile = keyPath
	cfg.TLSInsecureSkipVerify = true
	cfg.TLSServerName = "custom.kafka.cluster"

	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error building tls config: %v", err)
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Errorf("expected InsecureSkipVerify to be true")
	}
	if tlsConfig.ServerName != "custom.kafka.cluster" {
		t.Errorf("expected ServerName 'custom.kafka.cluster', got '%s'", tlsConfig.ServerName)
	}
	if tlsConfig.RootCAs == nil {
		t.Errorf("expected RootCAs to be populated")
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(tlsConfig.Certificates))
	}
}
