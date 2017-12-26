package sessions

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack"
	"microsoft.com/sigbench/util"
)

type MsgpackInvocation struct {
	MessageType  int32
	InvocationID string
	Target       string
	Params       []string
}

func (m *MsgpackInvocation) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(4)
	return enc.Encode(m.MessageType, m.InvocationID, m.Target, m.Params)
}

func (m *MsgpackInvocation) DecodeMsgpack(dec *msgpack.Decoder) error {
	dec.DecodeArrayLen()
	messageType, err := dec.DecodeInt32()
	if err != nil {
		log.Printf("Failed to decode message %v\n", dec)
		return err
	}
	m.MessageType = messageType
	if messageType == 1 {
		return dec.Decode(&m.InvocationID, &m.Target, &m.Params)
	}
	return nil
}

type SignalRCoreMsgpackConnection struct {
	userIndex             int64
	connectionInProgress  int64
	connectionEstablished int64
	connectionError       int64
	connectionCloseError  int64
	successCount          int64

	messageReceiveCount int64
	messageSendCount    int64

	latencyLessThan100ms     int64
	latencyLessThan500ms     int64
	latencyLessThan1000ms    int64
	latencyGreaterThan1000ms int64

	instanceHitCount []int64
}

func (s *SignalRCoreMsgpackConnection) Name() string {
	return "SignalRCore:MessagePackConnection"
}

func (s *SignalRCoreMsgpackConnection) Setup(map[string]string) error {
	s.instanceHitCount = make([]int64, maxInstances, maxInstances)
	return nil
}

func (s *SignalRCoreMsgpackConnection) logError(ctx *UserContext, msg string, err error) {
	log.Printf("[Error][%s] %s due to %s", ctx.UserId, msg, err)
	atomic.AddInt64(&s.connectionError, 1)
}

func (s *SignalRCoreMsgpackConnection) logLatency(latency int64) {
	// log.Println("Latency: ", latency)
	if latency < 100 {
		atomic.AddInt64(&s.latencyLessThan100ms, 1)
	} else if latency < 500 {
		atomic.AddInt64(&s.latencyLessThan500ms, 1)
	} else if latency < 1000 {
		atomic.AddInt64(&s.latencyLessThan1000ms, 1)
	} else {
		atomic.AddInt64(&s.latencyGreaterThan1000ms, 1)
	}
}

func (s *SignalRCoreMsgpackConnection) logHostInstance(ctx *UserContext, hostName string) error {
	hostInstanceId, err := util.GetVMSSInstanceId(hostName)
	if err != nil {
		s.logError(ctx, "Fail to decode host name "+hostName, err)
		return err
	}
	atomic.AddInt64(&s.instanceHitCount[hostInstanceId], 1)
	return nil
}

func writeMessage(c *websocket.Conn, bytes []byte) error {
	buffer := make([]byte, 0, 5+len(bytes))
	length := len(bytes)
	for length > 0 {
		current := byte(length & 0x7F)
		length >>= 7
		if length > 0 {
			current |= 0x80
		}
		buffer = append(buffer, current)
	}
	if len(buffer) == 0 {
		buffer = append(buffer, 0)
	}
	buffer = append(buffer, bytes...)
	return c.WriteMessage(websocket.BinaryMessage, buffer)
}

var numBitsToShift = []uint{0, 7, 14, 21, 28}

func parseMessage(bytes []byte) ([]byte, error) {
	moreBytes := true
	msgLen := 0
	numBytes := 0
	for moreBytes && numBytes < len(bytes) {
		byteRead := bytes[numBytes]
		msgLen = msgLen | int(uint(byteRead&0x7F)<<numBitsToShift[numBytes])
		numBytes++
		moreBytes = (byteRead & 0x80) != 0
	}

	if msgLen+numBytes > len(bytes) {
		return nil, fmt.Errorf("Not enough data in message, message length = %d, length section bytes = %d, data length = %d", msgLen, numBytes, len(bytes))
	}

	return bytes[numBytes : numBytes+msgLen], nil
}

func (s *SignalRCoreMsgpackConnection) Execute(ctx *UserContext) error {
	atomic.AddInt64(&s.connectionInProgress, 1)

	hosts := strings.Split(ctx.Params[ParamHost], ",")
	host := hosts[atomic.AddInt64(&s.userIndex, 1)%int64(len(hosts))]

	negotiateResponse, err := http.Post("http://"+host+"/chat/negotiate", "text/plain;charset=UTF-8", nil)
	if err != nil {
		s.logError(ctx, "Failed to negotiate with the server", err)
		return err
	}
	defer negotiateResponse.Body.Close()

	// Record host instance
	if err = s.logHostInstance(ctx, negotiateResponse.Header.Get("X-HostName")); err != nil {
		return err
	}

	decoder := json.NewDecoder(negotiateResponse.Body)
	var handshakeContent SignalRCoreHandshakeResp
	err = decoder.Decode(&handshakeContent)
	if err != nil {
		s.logError(ctx, "Failed to decode connection id", err)
		return err
	}

	wsURL := "ws://" + host + "/chat?id=" + handshakeContent.ConnectionId
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		s.logError(ctx, "Failed to connect to websocket", err)
		return err
	}
	defer c.Close()

	closeChan := make(chan int)

	go func() {
		defer func() {
			// abnormal close
			closeChan <- 1
			close(closeChan)
		}()
		established := false
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure) {
					s.logError(ctx, "Fail to read incoming message", err)
				} else {
					// normal close
					closeChan <- 0
				}
				return
			}
			if !established {
				atomic.AddInt64(&s.connectionInProgress, -1)
				atomic.AddInt64(&s.connectionEstablished, 1)
				established = true
			}
			msg, err = parseMessage(msg)
			if err != nil {
				s.logError(ctx, "Failed to parse message", err)
				return
			}

			var content MsgpackInvocation
			err = msgpack.Unmarshal(msg, &content)
			if err != nil {
				s.logError(ctx, "Failed to decode incoming message", err)
				return
			}
			atomic.AddInt64(&s.messageReceiveCount, 1)

			if content.Target == "echo" {
				sendStart, err := strconv.ParseInt(content.Params[1], 10, 64)
				if err != nil {
					s.logError(ctx, "Failed to decode start timestamp", err)
				} else {
					s.logLatency((time.Now().UnixNano() - sendStart) / 1000000)
				}
			}
		}
	}()

	err = c.WriteMessage(websocket.TextMessage, []byte("{\"protocol\":\"messagepack\"}\x1e"))
	if err != nil {
		s.logError(ctx, "Fail to set protocol", err)
		return err
	}

	invocationID := 0

	// Send message
	sendMessage := func() error {
		invocation := MsgpackInvocation{
			MessageType:  1,
			InvocationID: strconv.Itoa(invocationID),
			Target:       "echo",
			Params: []string{
				ctx.UserId,
				strconv.FormatInt(time.Now().UnixNano(), 10),
			},
		}
		msg, err := msgpack.Marshal(&invocation)
		if err != nil {
			s.logError(ctx, "Fail to pack signalr core message", err)
			return err
		}
		err = writeMessage(c, msg)
		if err != nil {
			s.logError(ctx, "Fail to send echo message", err)
			return err
		}
		invocationID++
		atomic.AddInt64(&s.messageSendCount, 1)
		return nil
	}
	if err = sendMessage(); err != nil {
		return err
	}
	repeatEcho := ctx.Params[ParamRepeatEcho]
	if repeatEcho == "true" {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			if err = sendMessage(); err != nil {
				ticker.Stop()
				return err
			}
		}
	}

	defer atomic.AddInt64(&s.connectionEstablished, -1)
	for {
		control, ok := <-ctx.Control
		log.Println("Control:", control, ", ok:", ok)
		if !ok || control == "close" {
			err = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				s.logError(ctx, "Fail to close websocket gracefully", err)
				return err
			}
			break
		}
	}

	closeState := <-closeChan
	if closeState != 0 {
		log.Println("Failed in websocket session")
		atomic.AddInt64(&s.connectionCloseError, 1)
	} else {
		atomic.AddInt64(&s.successCount, 1)
	}

	return nil
}

func (s *SignalRCoreMsgpackConnection) Counters() map[string]int64 {
	counters := map[string]int64{
		"signalrcore:msgpack:connection:inprogress":      atomic.LoadInt64(&s.connectionInProgress),
		"signalrcore:msgpack:connection:established":     atomic.LoadInt64(&s.connectionEstablished),
		"signalrcore:msgpack:connection:error":           atomic.LoadInt64(&s.connectionError),
		"signalrcore:msgpack:connection:closeerror":      atomic.LoadInt64(&s.connectionCloseError),
		"signalrcore:msgpack:connection:success":         atomic.LoadInt64(&s.successCount),
		"signalrcore:msgpack:connection:message:receive": atomic.LoadInt64(&s.messageReceiveCount),
		"signalrcore:msgpack:connection:message:send":    atomic.LoadInt64(&s.messageSendCount),
		"signalrcore:msgpack:connection:latency:lt_100":  atomic.LoadInt64(&s.latencyLessThan100ms),
		"signalrcore:msgpack:connection:latency:lt_500":  atomic.LoadInt64(&s.latencyLessThan500ms),
		"signalrcore:msgpack:connection:latency:lt_1000": atomic.LoadInt64(&s.latencyLessThan1000ms),
		"signalrcore:msgpack:connection:latency:gt_1000": atomic.LoadInt64(&s.latencyGreaterThan1000ms),
	}

	for i := 0; i < maxInstances; i++ {
		if val := atomic.LoadInt64(&s.instanceHitCount[i]); val > 0 {
			counters["signalrcore:connection:instancehit:"+strconv.Itoa(i)] = val
		}
	}

	return counters
}
