package neobench

import (
	"crypto/tls"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"io"
	"net/url"
)

type EncryptionMode int

const (
	EncryptionAuto EncryptionMode = 0
	EncryptionOff  EncryptionMode = 1
	EncryptionOn   EncryptionMode = 2
)

func NewDriver(urlStr, user, password string, encryptionMode EncryptionMode) (neo4j.Driver, error) {
	var encrypted bool
	switch encryptionMode {
	case EncryptionOff:
		encrypted = false
	case EncryptionOn:
		encrypted = true
	case EncryptionAuto:
		enabled, err := isTlsEnabled(urlStr)
		if err != nil {
			return nil, err
		}
		encrypted = enabled
	}

	config := func(conf *neo4j.Config) { conf.Encrypted = encrypted }
	return neo4j.NewDriver(urlStr, neo4j.BasicAuth(user, password, ""), config)
}

func isTlsEnabled(urlStr string) (bool, error) {
	parsedUrl, err := url.Parse(urlStr)
	if err != nil {
		return false, fmt.Errorf("invalid url: %s, %s", urlStr, err)
	}

	host := parsedUrl.Hostname()
	port := parsedUrl.Port()
	if port == "" {
		port = "7687"
	}

	socket, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", host, port), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, fmt.Errorf("failed to auto-detect TLS, consider explicitly setting the -e flag: %s", err)
	}
	socket.Close()
	return true, nil
}
