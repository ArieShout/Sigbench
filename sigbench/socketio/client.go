package socketio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var alphabet = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_")
var alphabetLength = int64(len(alphabet))
var engineIOProtocol = 3

const (
	Connecting = "Connecting"
	Connected  = "Connected"
	Closed     = "Closed"
)

// Yeast is a tiny growing id generator.
type Yeast struct {
	seed int64
	prev string
}

// Generate produces a unique ID correlated with the time.
func (yeast *Yeast) Generate() string {
	now := yeast.encode(time.Now().UnixNano() / 1000000)

	if now != yeast.prev {
		yeast.seed = 0
		yeast.prev = now
		return now
	}

	yeast.seed++
	return now + "." + yeast.encode(yeast.seed)
}

func (yeast *Yeast) encode(num int64) string {
	var encoded []byte

	for num > 0 {
		encoded = append(encoded, alphabet[int(num%alphabetLength)])
		num /= alphabetLength
	}

	for i, j := 0, len(encoded)-1; i < j; i, j = i+1, j-1 {
		encoded[i], encoded[j] = encoded[j], encoded[i]
	}

	return string(encoded)
}

type Message interface {
	Type() int
	Bytes() []byte
}

type PlainMessage struct {
	tpe     int
	message string
}

func (msg PlainMessage) Type() int {
	return msg.tpe
}

func (msg PlainMessage) Bytes() []byte {
	return []byte(msg.message)
}

type ObjectMessage struct {
	tpe    int
	object interface{}
}

func (msg ObjectMessage) Type() int {
	return msg.tpe
}

func (msg ObjectMessage) Bytes() []byte {
	bytes, err := json.Marshal(msg.object)
	if err != nil {
		return []byte{}
	}
	return bytes
}

type MessageHandler func(Message)
type StateChangeHandler func(string, string)

var messageTypeRe = regexp.MustCompile("^(\\d+)(.*)$")

// Client represents a socket.io client.
type Client struct {
	yeast              Yeast
	closeChan          chan struct{}
	writerClosed       chan struct{}
	writerQueue        chan Message
	State              string
	Conn               *websocket.Conn
	msgHandlers        []MessageHandler
	stateChangeHandler StateChangeHandler
	Handshake
}

// Handshake represents the initial socket.io HTTP handshake to determine the websocket configurations.
type Handshake struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int64    `json:"pingInterval"`
	PingTimeout  int64    `json:"pingTimeout"`
}

// Invocation is a representation of a SignalR invocation payload. We use this so that the message size is similar to SignalR.
type Invocation struct {
	Target string                 `json:"target"`
	Arg    map[string]interface{} `json:"arg"`
}

// NewClient creates a new socket.io client and connect to the remote server.
func NewClient(host string, handler MessageHandler, stateChangeHandler StateChangeHandler) (*Client, error) {
	var io = Client{
		closeChan:          make(chan struct{}),
		writerClosed:       make(chan struct{}),
		writerQueue:        make(chan Message),
		State:              Connecting,
		msgHandlers:        []MessageHandler{handler},
		stateChangeHandler: stateChangeHandler,
	}

	uri := fmt.Sprintf("http://%s/socket.io/?EIO=%d&transport=polling&t=%s", host, engineIOProtocol, io.yeast.Generate())
	handshakeResponse, err := http.Get(uri)
	if err != nil {
		return nil, err
	}
	defer handshakeResponse.Body.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(handshakeResponse.Body)
	body := buf.String()

	first, last := strings.IndexByte(body, '{'), strings.LastIndexByte(body, '}')
	if first < 0 || last < 0 || first+1 >= last {
		return nil, fmt.Errorf("Failed to do socket.io handshake")
	}
	bodyBytes := []byte(body)[first : last+1]

	err = json.Unmarshal(bodyBytes, &io.Handshake)
	if err != nil {
		return nil, err
	}

	wsURL := fmt.Sprintf("ws://%s/socket.io/?EIO=%d&transport=websocket&sid=%s", host, engineIOProtocol, io.SID)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, err
	}
	io.Conn = conn
	io.moveState(Connected)

	// WriteMessage go-routine
	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(io.PingInterval))
		defer ticker.Stop()
		defer close(io.writerClosed)
		for {
			select {
			case <-io.closeChan:
				return
			case msg := <-io.writerQueue:
				io.writeMessage(msg)
			case <-ticker.C:
				io.writeMessage(PlainMessage{2, ""})
			}
		}
	}()

	// ReadMessage go-routine
	go func() {
		for {
			_, msg, err := io.Conn.ReadMessage()
			if err != nil {
				return
			}

			groups := messageTypeRe.FindStringSubmatch(string(msg))
			if len(groups) == 3 {
				handlers := io.msgHandlers
				tpe, _ := strconv.Atoi(groups[1])
				msg := PlainMessage{tpe, groups[2]}
				for _, handler := range handlers {
					handler(msg)
				}
			}
		}
	}()

	io.SendStringMessage(2, "probe")
	io.SendStringMessage(5, "")

	return &io, nil
}

func (io *Client) moveState(newState string) {
	originalState := io.State
	io.State = newState
	if io.stateChangeHandler != nil {
		io.stateChangeHandler(originalState, newState)
	}
}

func (io *Client) writeMessage(msg Message) error {
	var buffer bytes.Buffer
	buffer.WriteString(strconv.Itoa(msg.Type()))
	if objectMsg, ok := msg.(ObjectMessage); ok {
		obj := objectMsg.object.(Invocation)
		obj.Arg["arguments"].([]string)[1] = strconv.FormatInt(time.Now().UnixNano(), 10)
		obj.Arg["p"] = 1 // extra payload so that socket.io message size is exactly the same as signalR
		data := []interface{}{
			obj.Target,
			obj.Arg,
		}
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		_, err = buffer.Write(bytes)
		if err != nil {
			return err
		}
	} else {
		buffer.Write(msg.Bytes())
	}
	err := io.Conn.WriteMessage(websocket.TextMessage, buffer.Bytes())
	return err
}

// SendStringMessage send a text message with the specific type to the remote socket.io server via the websocket.
func (io *Client) SendStringMessage(messageType int, message string) {
	io.writerQueue <- PlainMessage{messageType, message}
}

// SendObjectMessage send a message similar to the SignalR invocation.
func (io *Client) SendObjectMessage(messageType int, obj Invocation) {
	io.writerQueue <- ObjectMessage{messageType, obj}
}

// AddMessageHandler registers a handler that will be called when a server message is received.
func (io *Client) AddMessageHandler(handler MessageHandler) {
	io.msgHandlers = append(io.msgHandlers, handler)
}

// Close stops all the background workers for the client and gracefully closes the client.
func (io *Client) Close() {
	// TODO: we need to send websocket message to the server to indicate we want to close the socket
	close(io.closeChan)
	<-io.writerClosed
	io.Conn.Close()
	io.moveState(Closed)
}
