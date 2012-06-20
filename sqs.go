//
// goamz - Go packages to interact with the Amazon Web Services.
//
//   https://wiki.ubuntu.com/goamz
//
// Copyright (c) 2011 Memeo Inc.
//
// Written by Prudhvi Krishna Surapaneni <me@prudhvi.net>
//
package sqs

import (
	"net/http"
	"net/http/httputil"
	"encoding/xml"
	"net/url"
	"time"
	"fmt"
	"log"
	"strconv"
	"launchpad.net/goamz/aws"
)

const debug = false


// The SQS type encapsulates operation with an SQS region.
type SQS struct {
	aws.Auth
	aws.Region
	private byte // Reserve the right of using private data.
}

func New(auth aws.Auth, region aws.Region) *SQS {
	return &SQS{auth, region, 0}
}

type Queue struct {
	*SQS
	Url string
}

type CreateQueueResponse struct {
	QueueUrl         string `xml:"CreateQueueResult>QueueUrl"`
	ResponseMetadata ResponseMetadata
}

type GetQueueUrlResponse struct {
	QueueUrl         string `xml:"GetQueueUrlResult>QueueUrl"`
	ResponseMetadata ResponseMetadata
}

type ListQueuesResponse struct {
	QueueUrl         []string `xml:"ListQueuesResult>QueueUrl"`
	ResponseMetadata ResponseMetadata
}

type DeleteMessageResponse struct {
	ResponseMetadata ResponseMetadata
}


type DeleteQueueResponse struct {
	ResponseMetadata ResponseMetadata
}

type SendMessageResponse struct {
	MD5              string `xml:"SendMessageResult>MD5OfMessageBody"`
	Id               string `xml:"SendMessageResult>MessageId"`
	ResponseMetadata ResponseMetadata
}

type ReceiveMessageResponse struct {
	Messages         []Message `xml:"ReceiveMessageResult>Message"`
	ResponseMetadata ResponseMetadata
}

type Message struct {
	MessageId     string      `xml:"MessageId"`
	Body          string      `xml:"Body"`
	MD5OfBody     string      `xml:"MD5OfBody"`
	ReceiptHandle string      `xml:"ReceiptHandle"`
	Attribute     []Attribute `xml:"Attribute"`
}

type Attribute struct {
	Name  string `xml:"ReceiveMessageResult>Message>Attribute>Name"`
	Value string `xml:"ReceiveMessageResult>Message>Attribute>Value"`
}

type ChangeMessageVisibilityResponse struct {
	ResponseMetadata ResponseMetadata
}

type GetQueueAttributesResponse struct {
	Attributes       []Attribute `xml:"GetQueueAttributesResult>Attribute"`
	ResponseMetadata ResponseMetadata
}

type ResponseMetadata struct {
	RequestId string
	BoxUsage  float64
}

type Error struct {
	StatusCode int
	Code       string
	Message    string
	RequestId  string
}

func (err *Error) Error() string {
	if err.Code == "" {
		return err.Message
	}
	return fmt.Sprintf("%s (%s)", err.Message, err.Code)
}


func (err *Error) String() string {
	return err.Message
}

type xmlErrors struct {
	RequestId string
	Errors    []Error `xml:"Errors>Error"`
	Error     Error
}

func (s *SQS) CreateQueue(queueName string) (*Queue, error) {
	return s.CreateQueueWithTimeout(queueName, 30)
}

func (s *SQS) CreateQueueWithTimeout(queueName string, timeout int) (q *Queue, err error) {
	resp, err := s.newQueue(queueName, timeout)
	if err != nil {
		return nil, err
	}
	q = &Queue{s, resp.QueueUrl}
	return
}

func (s *SQS) GetQueue(queueName string) (*Queue, error) {
	var q *Queue
	resp, err := s.getQueueUrl(queueName)
	if err != nil {
		return q, err
	}
	q = &Queue{s, resp.QueueUrl}
	return q, nil
}


func (s *SQS) QueueFromArn(queueUrl string) (q *Queue) {
	q = &Queue{s, queueUrl}
	return
}


func (s *SQS) getQueueUrl(queueName string) (resp *GetQueueUrlResponse, err error) {
	resp = &GetQueueUrlResponse{}
	params := makeParams("GetQueueUrl")
	params["QueueName"] = queueName
	err = s.query("", params, resp)
	return resp, err
}

func (s *SQS) newQueue(queueName string, timeout int) (resp *CreateQueueResponse, err error) {
	resp = &CreateQueueResponse{}
	params := makeParams("CreateQueue")

	params["QueueName"] = queueName
	params["DefaultVisibilityTimeout"] = strconv.Itoa(timeout)

	err = s.query("", params, resp)
	return
}

func (s *SQS) ListQueues(QueueNamePrefix string) (resp *ListQueuesResponse, err error) {
	resp = &ListQueuesResponse{}
	params := makeParams("ListQueues")

	if QueueNamePrefix != "" {
		params["QueueNamePrefix"] = QueueNamePrefix
	}

	err = s.query("", params, resp)
	return
}

func (q *Queue) Delete() (resp *DeleteQueueResponse, err error) {
	resp = &DeleteQueueResponse{}
	params := makeParams("DeleteQueue")

	err = q.SQS.query(q.Url, params, resp)
	return
}

func (q *Queue) SendMessage(MessageBody string) (resp *SendMessageResponse, err error) {
	resp = &SendMessageResponse{}
	params := makeParams("SendMessage")

	params["MessageBody"] = MessageBody

	err = q.SQS.query(q.Url, params, resp)
	return
}

func (q *Queue) ReceiveMessage(MaxNumberOfMessages, VisibilityTimeoutSec int) (resp *ReceiveMessageResponse, err error) {
	resp = &ReceiveMessageResponse{}
	params := makeParams("ReceiveMessage")

	params["AttributeName"] = "All"
	params["MaxNumberOfMessages"] = strconv.Itoa(MaxNumberOfMessages)
	params["VisibilityTimeout"] = strconv.Itoa(VisibilityTimeoutSec)

	err = q.SQS.query(q.Url, params, resp)
	return
}

func (q *Queue) ChangeMessageVisibility(M *Message, VisibilityTimeout int) (resp *ChangeMessageVisibilityResponse, err error) {
	resp = &ChangeMessageVisibilityResponse{}
	params := makeParams("ChangeMessageVisibility")
	params["VisibilityTimeout"] = strconv.Itoa(VisibilityTimeout)
	params["ReceiptHandle"] = M.ReceiptHandle

	err = q.SQS.query(q.Url, params, resp)
	return
}

func (q *Queue) GetQueueAttributes(A string) (resp *GetQueueAttributesResponse, err error) {
	resp = &GetQueueAttributesResponse{}
	params := makeParams("GetQueueAttributes")
	params["AttributeName"] = A

	err = q.SQS.query(q.Url, params, resp)
	return
}

func (q *Queue) DeleteMessage(M *Message) (resp *DeleteMessageResponse, err error) {
	resp = &DeleteMessageResponse{}
	params := makeParams("DeleteMessage")
	params["ReceiptHandle"] = M.ReceiptHandle

	err = q.SQS.query(q.Url, params, resp)
	return
}

type SendMessageBatchResultEntry struct {
	Id               string `xml:"Id"`
	MessageId        string `xml:"MessageId"`
	MD5OfMessageBody string `xml:"MD5OfMessageBody"`
}

type SendMessageBatchResponse struct {
	SendMessageBatchResult []SendMessageBatchResultEntry `xml:"SendMessageBatchResult>SendMessageBatchResultEntry"`
	ResponseMetadata       ResponseMetadata
}

/* SendMessageBatch 
 */
func (q *Queue) SendMessageBatch(msgList []string) (resp *SendMessageBatchResponse, err error) {
	resp = &SendMessageBatchResponse{}
	params := makeParams("SendMessageBatch")

	for idx, msg := range msgList {
		count := idx + 1
		params[fmt.Sprintf("SendMessageBatchRequestEntry.%d.Id", count)] = fmt.Sprintf("msg-%d", count)
		params[fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageBody", count)] = msg
	}

	err = q.SQS.query(q.Url, params, resp)
	return
}

type DeleteMessageBatchResponse struct {
	DeleteMessageBatchResult []struct {
		Id          string
		SenderFault bool
		Code        string
		Message     string
	}                `xml:"DeleteMessageBatchResult>DeleteMessageBatchResultEntry"`
	ResponseMetadata ResponseMetadata
}

/* DeleteMessageBatch */
func (q *Queue) DeleteMessageBatch(msgList []*Message) (resp *DeleteMessageBatchResponse, err error) {
	resp = &DeleteMessageBatchResponse{}
	params := makeParams("DeleteMessageBatch")

	for idx, msg := range msgList {
		idx = idx + 1
		params[fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", idx)] = fmt.Sprintf("msg-%d", idx)
		params[fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", idx)] = msg.ReceiptHandle
	}

	err = q.SQS.query(q.Url, params, resp)
	return
}

func (s *SQS) query(queueUrl string, params map[string]string, resp interface{}) (err error) {
	params["Version"] = "2011-10-01"
	params["Timestamp"] = time.Now().In(time.UTC).Format(time.RFC3339)
	var url_ *url.URL

	var path string
	if queueUrl != "" {
		url_, err = url.Parse(queueUrl)
		path = queueUrl[len(s.Region.SQSEndpoint):]
	} else {
		url_, err = url.Parse(s.Region.SQSEndpoint)
		path = "/"
	}
	if err != nil {
		return err
	}

	//url_, err := url.Parse(s.Region.SQSEndpoint)
	//if err != nil {
	//	return err
	//}

	sign(s.Auth, "GET", path, params, url_.Host)

	url_.RawQuery = multimap(params).Encode()

	if debug {
		log.Printf("GET ", url_.String())
	}

	r, err := http.Get(url_.String())
	if err != nil {
		return err
	}

	defer r.Body.Close()

	if debug {
		dump, _ := httputil.DumpResponse(r, true)
		log.Printf("DUMP:\n", string(dump))
	}

	if r.StatusCode != 200 {
		return buildError(r)
	}
	err = xml.NewDecoder(r.Body).Decode(resp)
	return err
}

func buildError(r *http.Response) error {
	errors := xmlErrors{}
	xml.NewDecoder(r.Body).Decode(&errors)
	var err Error
	if len(errors.Errors) > 0 {
		err = errors.Errors[0]
	} else {
		err = errors.Error
	}
	err.RequestId = errors.RequestId
	err.StatusCode = r.StatusCode
	if err.Message == "" {
		err.Message = r.Status
	}
	return &err
}

func makeParams(action string) map[string]string {
	params := make(map[string]string)
	params["Action"] = action
	return params
}

func multimap(p map[string]string) url.Values {
	q := make(url.Values, len(p))
	for k, v := range p {
		q[k] = []string{v}
	}
	return q
}
