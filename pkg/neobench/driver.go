package neobench

import (
	"crypto/tls"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"io"
	"net/url"
	"strings"
)

type EncryptionMode int

const (
	EncryptionAuto EncryptionMode = 0
	EncryptionOff  EncryptionMode = 1
	EncryptionOn   EncryptionMode = 2
)

func NewDriver(urlStr, user, password string, encryptionMode EncryptionMode, checkCertificates bool,
	configurers ...func(*neo4j.Config)) (neo4j.Driver, error) {

	if encryptionMode == EncryptionAuto {
		enabled, err := isTlsEnabled(urlStr)
		if err != nil {
			return nil, err
		}
		if enabled {
			encryptionMode = EncryptionOn
		} else {
			encryptionMode = EncryptionOff
		}
	}

	switch encryptionMode {
	case EncryptionOff:
		urlStr = "neo4j://" + strings.SplitN(urlStr, "://", 2)[1]
	case EncryptionOn:
		if checkCertificates {
			urlStr = "neo4j+s://" + strings.SplitN(urlStr, "://", 2)[1]
		} else {
			urlStr = "neo4j+ssc://" + strings.SplitN(urlStr, "://", 2)[1]
		}
	case EncryptionAuto:
		panic("this should not be reached")
	}

	return neo4j.NewDriver(urlStr, neo4j.BasicAuth(user, password, ""), configurers...)
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
