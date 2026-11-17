package forward

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

// LoadTLSConfig 从 PEM 文件构造双向 TLS 配置（LLD §8.2）。
// 密钥不被记录到日志，也不从配置文件读取内容（只读文件路径）。
func LoadTLSConfig(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("forward: load key pair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("forward: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("forward: invalid CA PEM")
	}
	return buildTLSConfig(cert, pool, serverName), nil
}

func buildTLSConfig(cert tls.Certificate, pool *x509.CertPool, serverName string) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   serverName,
	}
}

// GenerateDevCA 生成一套临时自签 CA，返回证书与私钥以便继续签发叶证书。
// 仅供本地开发/验收；生产必须使用内部 CA（LLD §8.3）。
func GenerateDevCA(caFile, caKeyFile string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "RelayX Dev CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, err
	}
	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		return nil, nil, err
	}
	if err := writePEM(caFile, "CERTIFICATE", caDER); err != nil {
		return nil, nil, err
	}
	if err := writePEM(caKeyFile, "EC PRIVATE KEY", caKeyDER); err != nil {
		return nil, nil, err
	}
	return caCert, caKey, nil
}

// GenerateDevLeaf 用给定 CA 为一个节点签发双向叶证书。
// dnsName 通常传 "localhost"（也自动包含），另外固定加入回环 IP SAN
// 127.0.0.1 / ::1，这样默认的 127.0.0.1 监听地址无需显式 ServerName 也能校验通过。
func GenerateDevLeaf(caCert *x509.Certificate, caKey *ecdsa.PrivateKey, nodeID, dnsName, certFile, keyFile string) error {
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	dnsNames := []string{"localhost"}
	if dnsName != "" && dnsName != "localhost" && net.ParseIP(dnsName) == nil {
		dnsNames = append(dnsNames, dnsName)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return err
	}
	if err := writePEM(certFile, "CERTIFICATE", leafDER); err != nil {
		return err
	}
	return writePEM(keyFile, "EC PRIVATE KEY", leafKeyDER)
}

// EnsureDevCA 复用已存在的开发 CA，缺失或不可用时才重新生成。
// 多节点本地运行必须共享同一 CA，否则节点间 mTLS 互信失败。
func EnsureDevCA(caFile, caKeyFile string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	if cert, key, err := loadDevCA(caFile, caKeyFile); err == nil {
		return cert, key, nil
	}
	return GenerateDevCA(caFile, caKeyFile)
}

func loadDevCA(caFile, caKeyFile string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, err
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return nil, nil, errors.New("forward: invalid dev CA certificate PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	if !cert.IsCA || !time.Now().Before(cert.NotAfter) {
		return nil, nil, errors.New("forward: dev CA is not usable")
	}
	keyPEM, err := os.ReadFile(caKeyFile)
	if err != nil {
		return nil, nil, err
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, errors.New("forward: invalid dev CA key PEM")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || !pub.Equal(&key.PublicKey) {
		return nil, nil, errors.New("forward: dev CA key does not match certificate")
	}
	return cert, key, nil
}

// GenerateDevCertificates 生成"一台节点"的叶证书；CA 已存在时复用，
// 保证多次/多节点调用互信（单节点本地运行快捷路径）。
func GenerateDevCertificates(certFile, keyFile, caFile, nodeID, dnsName string) error {
	caCert, caKey, err := EnsureDevCA(caFile, caFile+".key")
	if err != nil {
		return err
	}
	return GenerateDevLeaf(caCert, caKey, nodeID, dnsName, certFile, keyFile)
}

func writePEM(path, typ string, der []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
