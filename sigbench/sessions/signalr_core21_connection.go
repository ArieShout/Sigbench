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

type SignalRCore21Connection struct {
	userIndex             int64
	connectionInProgress  int64
	connectionEstablished int64
	connectionError       int64
	connectionCloseError  int64
	successCount          int64

	messageReceiveCount int64
	messageSendCount    int64

	latency [11]int64

	instanceHitCount []int64
}

func (s *SignalRCore21Connection) Name() string {
	return "SignalRCore21:Connection"
}

func (s *SignalRCore21Connection) Setup(map[string]string) error {
	s.instanceHitCount = make([]int64, maxInstances, maxInstances)
	return nil
}

func (s *SignalRCore21Connection) logError(ctx *UserContext, msg string, err error) {
	log.Printf("[Error][%s] %s due to %s", ctx.UserId, msg, err)
	atomic.AddInt64(&s.connectionError, 1)
}

func (s *SignalRCore21Connection) logLatency(latency int64) {
	index := int(latency / 100)
	if index > 10 {
		index = 10
	}
	atomic.AddInt64(&s.latency[index], 1)
}

func (s *SignalRCore21Connection) logHostInstance(ctx *UserContext, hostName string) error {
	hostInstanceId, err := util.GetVMSSInstanceId(hostName)
	if err != nil {
		s.logError(ctx, "Fail to decode host name "+hostName, err)
		return err
	}
	atomic.AddInt64(&s.instanceHitCount[hostInstanceId], 1)
	return nil
}

func (s *SignalRCore21Connection) Execute(ctx *UserContext) error {
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
			if !established {
				atomic.AddInt64(&s.connectionInProgress, -1)
				atomic.AddInt64(&s.connectionEstablished, 1)
				established = true
			}

			msg := msgWithTerm[:len(msgWithTerm)-1]
			var content SignalRCoreInvocation
			err = json.Unmarshal(msg, &content)
			if err != nil {
				s.logError(ctx, "Failed to decode incoming message", err)
				return
			}

			atomic.AddInt64(&s.messageReceiveCount, 1)

			if content.Type == 1 && content.Target == "echo" {
				sendStart, err := strconv.ParseInt(content.Arguments[1], 10, 64)
				if err != nil {
					s.logError(ctx, "Failed to decode start timestamp", err)
				} else {
					s.logLatency((time.Now().UnixNano() - sendStart) / 1000000)
				}
			}

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

	invocationID := 0

	// Send message
	sendMessage := func() error {
		msg, err := SerializeSignalRCoreMessage(&SignalRCoreInvocation{
			Type:         1,
			InvocationId: strconv.Itoa(invocationID),
			Target:       "echo",
			Arguments: []string{
				ctx.UserId,
				strconv.FormatInt(time.Now().UnixNano(), 10),
			},
			NonBlocking: false,
		})
		if err != nil {
			s.logError(ctx, "Fail to serialize signalr core message", err)
			return err
		}
		err = c.WriteMessage(websocket.TextMessage, msg)
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

func (s *SignalRCore21Connection) Counters() map[string]int64 {
	counters := map[string]int64{
		"signalrcore:connection:inprogress":      atomic.LoadInt64(&s.connectionInProgress),
		"signalrcore:connection:established":     atomic.LoadInt64(&s.connectionEstablished),
		"signalrcore:connection:error":           atomic.LoadInt64(&s.connectionError),
		"signalrcore:connection:closeerror":      atomic.LoadInt64(&s.connectionCloseError),
		"signalrcore:connection:success":         atomic.LoadInt64(&s.successCount),
		"signalrcore:connection:message:receive": atomic.LoadInt64(&s.messageReceiveCount),
		"signalrcore:connection:message:send":    atomic.LoadInt64(&s.messageSendCount),
		"signalrcore:connection:latency:gt_1000": atomic.LoadInt64(&s.latency[10]),
	}

	for i := 0; i < 10; i++ {
		counters["signalrcore:connection:latency:lt_"+strconv.Itoa(i+1)+"00"] = atomic.LoadInt64(&s.latency[i])
	}

	for i := 0; i < maxInstances; i++ {
		if val := atomic.LoadInt64(&s.instanceHitCount[i]); val > 0 {
			counters["signalrcore:connection:instancehit:"+strconv.Itoa(i)] = val
		}
	}

	return counters
}
