package certificates

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/pkg/errors"
)

func GenerateSignedServerCert(caCert *x509.Certificate, caKey *ecdsa.PrivateKey) (*tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		panic(err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(7 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create signed server certificate")
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse cert from bytes")
	}

	return selfsignedcert.ConstructTlsCert(cert, priv)
}

func GenerateSignedClientCert(caCert *x509.Certificate, caKey *ecdsa.PrivateKey, username string) (*tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		panic(err)
	}

	template := x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(7 * 24 * time.Hour),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		EmailAddresses:        []string{fmt.Sprintf("%s@couchbase.com", username)},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create singed server certificate")
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse cert from bytes")
	}

	return selfsignedcert.ConstructTlsCert(cert, priv)
}
