package selfsignedcert

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/pkg/errors"
)

func GenerateCertificate() (*x509.Certificate, *ecdsa.PrivateKey, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		return nil, nil, errors.New("failed to generate private key")
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to generate serial number")
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},

		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(7 * 24 * time.Hour),

		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create certificate")
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to parse cert from bytes")
	}

	return cert, priv, nil
}

func ConstructTlsCert(cert *x509.Certificate, key *ecdsa.PrivateKey) (*tls.Certificate, error) {
	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal private key")
	}

	keyBuf := bytes.NewBuffer(nil)
	err = pem.Encode(keyBuf, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to write key pem data")
	}

	certBuf := bytes.NewBuffer(nil)
	err = pem.Encode(certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	if err != nil {
		return nil, errors.Wrap(err, "failed to write cert pem data")
	}

	tlsCert, err := tls.X509KeyPair(certBuf.Bytes(), keyBuf.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "failed to produce tls certificate")
	}

	return &tlsCert, nil
}
