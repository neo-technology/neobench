package neobench

import (
	"crypto/tls"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/pkg/errors"
	"io"
	"net/url"
)

type EncryptionMode int

const (
	EncryptionAuto EncryptionMode = 0
	EncryptionOff  EncryptionMode = 1
	EncryptionOn   EncryptionMode = 2
)

func NewDriver(urlStr, user, password string, encryptionMode EncryptionMode, checkCertificates bool,
	configurers ...func(*neo4j.Config)) (neo4j.Driver, error) {

	urlStr, err := determineConnectionUrl(urlStr, encryptionMode, checkCertificates)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to determine connection URL to use from %s", urlStr)
	}

	return neo4j.NewDriver(urlStr, neo4j.BasicAuth(user, password, ""), configurers...)
}

// Modifies the input URL to match encryption and certificate check requirements; by default this is done automatically
func determineConnectionUrl(urlStr string, encryptionMode EncryptionMode, checkCertificates bool) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to parse url %s", urlStr)
	}

	if u.Scheme == "bolt+unix" {
		return urlStr, nil
	}

	if encryptionMode == EncryptionAuto {
		enabled, err := isTlsEnabled(u)
		if err != nil {
			return "", err
		}
		if enabled {
			encryptionMode = EncryptionOn
		} else {
			encryptionMode = EncryptionOff
		}
	}

	switch encryptionMode {
	case EncryptionOff:
		u.Scheme = "neo4j"
	case EncryptionOn:
		if checkCertificates {
			u.Scheme = "neo4j+s"
		} else {
			u.Scheme = "neo4j+ssc"
		}
	case EncryptionAuto:
		panic("this should not be reached")
	}

	return u.String(), nil
}

func isTlsEnabled(u *url.URL) (bool, error) {
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "7687"
	}

	socket, err := tls.Dial("tcp", fmt.Sprintf("%s:%s", host, port), &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         host,
	})
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, fmt.Errorf("failed to auto-detect TLS, consider explicitly setting the -e flag: %s", err)
	}
	socket.Close()
	return true, nil
}
