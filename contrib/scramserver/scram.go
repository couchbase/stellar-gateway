package scramserver

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"strconv"
)

// scramServer is a server implementation of SCRAM auth.
type scramServer struct {
	n                          []byte
	out                        bytes.Buffer
	serverFirstMsg             []byte
	clientFirstMsgBare         []byte
	clientFinalMsgWithoutProof []byte
	clientNonce                []byte
	salt                       []byte
	hashFn                     func() hash.Hash
	saltedPassword             []byte

	Username string
}

var b64 = base64.StdEncoding

const iterCount = 4096

// newScramServer creates a new ScramServer
func newScramServer(hashFnName string) (*scramServer, error) {
	hashFn, err := parseHashFn(hashFnName)
	if err != nil {
		return nil, err
	}

	nonceLen := 6
	buf := make([]byte, nonceLen+b64.EncodedLen(nonceLen))
	if _, err := rand.Read(buf[:nonceLen]); err != nil {
		return nil, fmt.Errorf("cannot read random from operating system: %v", err)
	}
	n := buf[nonceLen:]
	b64.Encode(n, buf[:nonceLen])

	saltLen := 10
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("cannot read random from operating system: %v", err)
	}

	s := &scramServer{
		n:      n,
		salt:   salt,
		hashFn: hashFn,
	}
	s.out.Grow(256)

	return s, nil
}

func newScramServerWithSaltAndNonce(hashFnName, salt, nonce string) (*scramServer, error) {
	hashFn, err := parseHashFn(hashFnName)
	if err != nil {
		return nil, err
	}

	s := &scramServer{
		n:      []byte(nonce),
		salt:   []byte(salt),
		hashFn: hashFn,
	}
	s.out.Grow(256)

	return s, nil
}

func parseHashFn(hashFnName string) (func() hash.Hash, error) {
	var hashFn func() hash.Hash
	switch hashFnName {
	case "SCRAM-SHA512":
		hashFn = sha512.New
	case "SCRAM-SHA256":
		hashFn = sha256.New
	case "SCRAM-SHA1":
		hashFn = sha1.New
	default:
		return nil, fmt.Errorf("unknown hash function: %s", hashFnName)
	}

	return hashFn, nil
}

func (s *scramServer) authMessage() []byte {
	var msg bytes.Buffer
	msg.Grow(256)
	msg.Write(s.clientFirstMsgBare)
	msg.WriteString(",")
	msg.Write(s.serverFirstMsg)
	msg.WriteString(",")
	msg.Write(s.clientFinalMsgWithoutProof)

	return msg.Bytes()
}

func (s *scramServer) clientProof() ([]byte, error) {
	mac := hmac.New(s.hashFn, s.saltedPassword)
	if _, err := mac.Write([]byte("Client Key")); err != nil {
		return nil, err
	}
	clientKey := mac.Sum(nil)
	hash := s.hashFn()
	if _, err := hash.Write(clientKey); err != nil {
		return nil, err
	}
	storedKey := hash.Sum(nil)
	mac = hmac.New(s.hashFn, storedKey)
	if _, err := mac.Write(s.authMessage()); err != nil {
		return nil, err
	}
	clientProof := mac.Sum(nil)
	for i, b := range clientKey {
		clientProof[i] ^= b
	}
	clientProof64 := make([]byte, b64.EncodedLen(len(clientProof)))
	b64.Encode(clientProof64, clientProof)
	return clientProof64, nil
}

func (s *scramServer) serverSignature() ([]byte, error) {
	mac := hmac.New(s.hashFn, s.saltedPassword)
	if _, err := mac.Write([]byte("Server Key")); err != nil {
		return nil, err
	}
	serverKey := mac.Sum(nil)

	mac = hmac.New(s.hashFn, serverKey)
	if _, err := mac.Write(s.authMessage()); err != nil {
		return nil, err
	}
	serverSignature := mac.Sum(nil)

	encoded := make([]byte, b64.EncodedLen(len(serverSignature)))
	b64.Encode(encoded, serverSignature)
	return encoded, nil
}

// Start performs the first step of the process, given request data from a client.
func (s *scramServer) Start(in []byte) (string, error) {
	s.out.Reset()
	fields := bytes.Split(in, []byte(","))
	if len(fields) != 4 {
		return "", fmt.Errorf("expected 4 fields in first SCRAM-SHA-1 client message, got %d: %q", len(fields), in)
	}
	if len(fields[0]) != 1 || fields[0][0] != 'n' {
		return "", fmt.Errorf("client sent an invalid SCRAM-SHA-1 message start: %q", fields[0])
	}
	// We ignore fields[1]
	if !bytes.HasPrefix(fields[2], []byte("n=")) || len(fields[2]) < 2 {
		return "", fmt.Errorf("server sent an invalid SCRAM-SHA-1 username: %q", fields[2])
	}
	if !bytes.HasPrefix(fields[3], []byte("r=")) || len(fields[3]) < 6 {
		return "", fmt.Errorf("client sent an invalid SCRAM-SHA-1 nonce: %q", fields[3])
	}

	idx := bytes.Index(in, []byte("n="))
	s.clientFirstMsgBare = make([]byte, len(in[idx:]))
	copy(s.clientFirstMsgBare, in[idx:])

	username := make([]byte, len(fields[2][2:]))
	copy(username, fields[2][2:])

	s.clientNonce = fields[3][2:]
	s.out.WriteString("r=")
	s.out.Write(s.clientNonce)
	s.out.Write(s.n)

	encodedSalt := make([]byte, b64.EncodedLen(len(s.salt)))
	b64.Encode(encodedSalt, s.salt)
	s.out.WriteString(",s=")
	s.out.Write(encodedSalt)

	s.out.WriteString(",i=")
	s.out.Write([]byte(strconv.Itoa(iterCount)))

	s.serverFirstMsg = make([]byte, s.out.Len())
	copy(s.serverFirstMsg, s.out.Bytes())

	return string(username), nil
}

// Step1 performs the first "step".
func (s *scramServer) Step1(in []byte) error {
	s.out.Reset()
	fields := bytes.Split(in, []byte(","))
	if len(fields) != 3 {
		return fmt.Errorf("expected 3 fields in first SCRAM-SHA-1 client message, got %d: %q", len(fields), in)
	}
	// We ignore fields[0]
	if !bytes.HasPrefix(fields[1], []byte("r=")) || len(fields[1]) < 6 {
		return fmt.Errorf("client sent an invalid SCRAM-SHA-1 nonce: %q", fields[3])
	}
	if !bytes.HasPrefix(fields[2], []byte("p=")) || len(fields[2]) < 6 {
		return fmt.Errorf("client sent an invalid SCRAM-SHA-1 proof: %q", fields[3])
	}

	rcvdN := fields[1][2:]
	var n bytes.Buffer
	n.Write(s.clientNonce)
	n.Write(s.n)
	if !bytes.Equal(rcvdN, n.Bytes()) {
		return fmt.Errorf("client sent an invalid nonce: %s != %s", rcvdN, n.Bytes())
	}
	idx := bytes.Index(in, []byte(",p="))
	s.clientFinalMsgWithoutProof = in[:idx]

	rvdClientProof := fields[2][2:]
	ourClientProof, err := s.clientProof()
	if err != nil {
		return err
	}

	if !bytes.Equal(rvdClientProof, ourClientProof) {
		return fmt.Errorf("client proof did not match our proof: %q != %q", rvdClientProof, ourClientProof)
	}

	srvSig, err := s.serverSignature()
	if err != nil {
		return err
	}

	s.out.WriteString("v=")
	s.out.Write(srvSig)

	return nil
}

// Out returns the current data buffer which can be sent to the client.
func (s *scramServer) Out() []byte {
	if s.out.Len() == 0 {
		return nil
	}
	return s.out.Bytes()
}

// SetPassword sets the salted password retrieved from the store for the username.
func (s *scramServer) SetPassword(password []byte) error {
	return s.saltPassword(password)
}

func (s *scramServer) saltPassword(password []byte) error {
	mac := hmac.New(s.hashFn, password)
	if _, err := mac.Write(s.salt); err != nil {
		return err
	}
	if _, err := mac.Write([]byte{0, 0, 0, 1}); err != nil {
		return err
	}
	ui := mac.Sum(nil)
	hi := make([]byte, len(ui))
	copy(hi, ui)
	for i := 1; i < iterCount; i++ {
		mac.Reset()
		if _, err := mac.Write(ui); err != nil {
			return err
		}
		mac.Sum(ui[:0])
		for j, b := range ui {
			hi[j] ^= b
		}
	}
	s.saltedPassword = hi
	return nil
}

// ScramServer is a server implementation of SCRAM auth with a slightly improved interface.
type ScramServer struct {
	srv      *scramServer
	username string
	password string
}

// Start performs the first step of the SCRAM process.  Returns nil if SCRAM completes.
func (s *ScramServer) Start(in []byte, hashFn string) ([]byte, error) {
	srv, err := newScramServer(hashFn)
	if err != nil {
		return nil, err
	}

	username, err := srv.Start(in)
	if err != nil {
		return nil, err
	}

	s.srv = srv
	s.username = username
	return srv.Out(), nil
}

// Step performs one step of the SCRAM process. Returns nil if SCRAM completes.
func (s *ScramServer) Step(in []byte) ([]byte, error) {
	if s.srv == nil {
		return nil, errors.New("scram must be started first")
	}

	// We blindly call Step1 here since other steps aren't possible to achieve
	// since we clear out the srv after step 1 completes.
	err := s.srv.Step1(in)
	if err != nil {
		s.srv = nil
		return nil, err
	}

	return s.srv.Out(), nil
}

// Username returns the password which was produced by SCRAM.
func (s *ScramServer) Username() string {
	return s.username
}

// Password returns the password which was produced by SCRAM.
func (s *ScramServer) Password() string {
	return s.password
}

// SetPassword sets the salted password retrieved from the store for the username.
func (s *ScramServer) SetPassword(password string) error {
	return s.srv.SetPassword([]byte(password))
}
