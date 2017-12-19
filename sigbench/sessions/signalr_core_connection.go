package sessions

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"microsoft.com/sigbench/util"
)

const maxInstances = 256

type SignalRCoreConnection struct {
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

func (s *SignalRCoreConnection) Name() string {
	return "SignalRCore:Connection"
}

func (s *SignalRCoreConnection) Setup(map[string]string) error {
	s.instanceHitCount = make([]int64, maxInstances, maxInstances)
	return nil
}

func (s *SignalRCoreConnection) logError(ctx *UserContext, msg string, err error) {
	log.Printf("[Error][%s] %s due to %s", ctx.UserId, msg, err)
	atomic.AddInt64(&s.connectionError, 1)
}

func (s *SignalRCoreConnection) logLatency(latency int64) {
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

func (s *SignalRCoreConnection) logHostInstance(ctx *UserContext, hostName string) error {
	hostInstanceId, err := util.GetVMSSInstanceId(hostName)
	if err != nil {
		s.logError(ctx, "Fail to decode host name "+hostName, err)
		return err
	}
	atomic.AddInt64(&s.instanceHitCount[hostInstanceId], 1)
	return nil
}

func (s *SignalRCoreConnection) Execute(ctx *UserContext) error {
	atomic.AddInt64(&s.connectionInProgress, 1)
	defer atomic.AddInt64(&s.connectionInProgress, 1)

	hosts := strings.Split(ctx.Params[ParamHost], ",")
	host := hosts[atomic.AddInt64(&s.userIndex, 1)%int64(len(hosts))]

	handshakeRequest, err := http.NewRequest(http.MethodOptions, "http://"+host+"/chat", nil)
	if err != nil {
		s.logError(ctx, "Failed to construct handshake request", err)
		return err
	}

	handshakeResponse, err := http.DefaultClient.Do(handshakeRequest)
	if err != nil {
		s.logError(ctx, "Failed to obtain connection id", err)
		return err
	}
	defer handshakeResponse.Body.Close()

	// Record host instance
	if err = s.logHostInstance(ctx, handshakeResponse.Header.Get("X-HostName")); err != nil {
		return err
	}

	decoder := json.NewDecoder(handshakeResponse.Body)
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
	defer close(closeChan)

	go func() {
		defer func() {
			// abnormal close
			closeChan <- 1
		}()
		for {
			_, msgWithTerm, err := c.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure) {
					s.logError(ctx, "Fail to read incoming message", err)
				} else {
					// normal close
					closeChan <- 0
				}
				return
			}

			msg := msgWithTerm[:len(msgWithTerm)-1]
			var content SignalRCoreInvocation
			err = json.Unmarshal(msg, &content)
			if err != nil {
				s.logError(ctx, "Failed to decode incoming message", err)
				return
			}

			atomic.AddInt64(&s.messageReceiveCount, 1)

			if content.Type == 1 && content.Target == "broadcast" {
				sendStart, err := strconv.ParseInt(content.Arguments[1], 10, 64)
				if err != nil {
					s.logError(ctx, "Failed to decode start timestamp", err)
				} else {
					s.logLatency((time.Now().UnixNano() - sendStart) / 1000000)
				}
			}
		}
	}()

	err = c.WriteMessage(websocket.TextMessage, []byte("{\"protocol\":\"json\"}\x1e"))
	if err != nil {
		s.logError(ctx, "Fail to set protocol", err)
		return err
	}

	atomic.AddInt64(&s.connectionEstablished, 1)
	defer atomic.AddInt64(&s.connectionEstablished, -1)

	for {
		control, ok := <-ctx.Control
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

func (s *SignalRCoreConnection) Counters() map[string]int64 {
	counters := map[string]int64{
		"signalrcore:connection:inprogress":      atomic.LoadInt64(&s.connectionInProgress),
		"signalrcore:connection:established":     atomic.LoadInt64(&s.connectionEstablished),
		"signalrcore:connection:error":           atomic.LoadInt64(&s.connectionError),
		"signalrcore:connection:closeerror":      atomic.LoadInt64(&s.connectionCloseError),
		"signalrcore:connection:success":         atomic.LoadInt64(&s.successCount),
		"signalrcore:connection:message:receive": atomic.LoadInt64(&s.messageReceiveCount),
		"signalrcore:connection:message:send":    atomic.LoadInt64(&s.messageSendCount),
		"signalrcore:connection:latency:<100":    atomic.LoadInt64(&s.latencyLessThan100ms),
		"signalrcore:connection:latency:<500":    atomic.LoadInt64(&s.latencyLessThan500ms),
		"signalrcore:connection:latency:<1000":   atomic.LoadInt64(&s.latencyLessThan1000ms),
		"signalrcore:connection:latency:>1000":   atomic.LoadInt64(&s.latencyGreaterThan1000ms),
	}

	for i := 0; i < maxInstances; i++ {
		if val := atomic.LoadInt64(&s.instanceHitCount[i]); val > 0 {
			counters["signalrcore:connection:instancehit:"+strconv.Itoa(i)] = val
		}
	}

	return counters
}
