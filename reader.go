// this is a synchronous version of github.com/mikedewar/go-sqsReader

package sqsReader

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/mikedewar/aws4"
	"io/ioutil"
	"net/url"
	"strings"
)

type sqsMessage struct {
	Body          []string `xml:"ReceiveMessageResult>Message>Body"`
	ReceiptHandle []string `xml:"ReceiveMessageResult>Message>ReceiptHandle"`
}

type Reader struct {
	client           *aws4.Client
	sqsEndpoint      string
	version          string
	signatureVersion string
	waitTime         string
	maxMsgs          string
	QuitChan         chan bool   // stops the reader
	OutChan          chan []byte // output channel for the client
}

func NewReader(sqsEndpoint, accessKey, accessSecret string, outChan chan []byte) *Reader {
	// ensure that the sqsEndpoint has a ? at the end
	if !strings.HasSuffix(sqsEndpoint, "?") {
		sqsEndpoint += "?"
	}
	AWSSQSAPIVersion := "2012-11-05"
	AWSSignatureVersion := "4"
	keys := &aws4.Keys{
		AccessKey: accessKey,
		SecretKey: accessSecret,
	}
	c := &aws4.Client{Keys: keys}
	// channels
	r := &Reader{
		client:           c,
		sqsEndpoint:      sqsEndpoint,
		version:          AWSSQSAPIVersion,
		signatureVersion: AWSSignatureVersion,
		waitTime:         "0",  // in seconds
		maxMsgs:          "10", // in messages
		QuitChan:         make(chan bool),
		OutChan:          outChan,
	}
	return r
}

func (r *Reader) Start() {

	query := url.Values{}
	query.Set("Action", "ReceiveMessage")
	query.Set("AttributeName", "All")
	query.Set("Version", r.version)
	query.Set("SignatureVersion", r.signatureVersion)
	query.Set("WaitTimeSeconds", r.waitTime)
	query.Set("MaxNumberOfMessages", r.maxMsgs)
	queryurl := r.sqsEndpoint + query.Encode()

	for {
		select {
		case <-r.QuitChan:
			return
		default:
			var m sqsMessage
			var m1 map[string]interface{}

			resp, err := r.client.Get(queryurl)

			if err != nil {
				continue
			}

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				continue
			}

			resp.Body.Close()

			err = xml.Unmarshal(body, &m)
			if err != nil {
				continue
			}

			if len(m.Body) == 0 {
				continue
			}

			for _, body := range m.Body {
				err = json.Unmarshal([]byte(body), &m1)
				if err != nil {
					continue
				}
				msgString, ok := m1["Message"].(string)
				if !ok {
					continue
				}
				msgs := strings.Split(msgString, "\n")
				for _, msg := range msgs {
					if len(msg) == 0 {
						continue
					}
					r.OutChan <- []byte(msg)
				}
			}

			delquery := url.Values{}
			delquery.Set("Action", "DeleteMessageBatch")
			delquery.Set("Version", r.version)
			delquery.Set("SignatureVersion", r.signatureVersion)
			for i, r := range m.ReceiptHandle {
				id := fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", (i + 1))
				receipt := fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", (i + 1))
				delquery.Add(id, fmt.Sprintf("msg%d", (i+1)))
				delquery.Add(receipt, r)
			}
			delurl := r.sqsEndpoint + delquery.Encode()

			resp, err = r.client.Get(delurl)
			if err != nil {
				continue
			}

			resp.Body.Close()
		}
	}
}

func (r *Reader) Stop() {
	r.QuitChan <- true
}
