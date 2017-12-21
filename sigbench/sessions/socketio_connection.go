package sessions

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"microsoft.com/sigbench/socketio"

	"microsoft.com/sigbench/util"
)

type SocketIOConnection struct {
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

func (s *SocketIOConnection) Name() string {
	return "SocketIOConnection:Connection"
}

func (s *SocketIOConnection) Setup(map[string]string) error {
	s.instanceHitCount = make([]int64, MaxInstances, MaxInstances)
	return nil
}

func (s *SocketIOConnection) logError(ctx *UserContext, msg string, err error) {
	log.Printf("[Error][%s] %s due to %s", ctx.UserId, msg, err)
	atomic.AddInt64(&s.connectionError, 1)
}

func (s *SocketIOConnection) logLatency(latency int64) {
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

func (s *SocketIOConnection) logHostInstance(ctx *UserContext, hostName string) error {
	hostInstanceId, err := util.GetVMSSInstanceId(hostName)
	if err != nil {
		s.logError(ctx, "Fail to decode host name "+hostName, err)
		return err
	}
	atomic.AddInt64(&s.instanceHitCount[hostInstanceId], 1)
	return nil
}

func (s *SocketIOConnection) Execute(ctx *UserContext) error {
	atomic.AddInt64(&s.connectionInProgress, 1)

	hosts := strings.Split(ctx.Params[ParamHost], ",")
	host := hosts[atomic.AddInt64(&s.userIndex, 1)%int64(len(hosts))]

	client, err := socketio.NewClient(host, func(msg socketio.Message) {
		if tpe := msg.Type(); tpe == 42 {
			var data []interface{}
			json.Unmarshal(msg.Bytes(), &data)
			if len(data) == 2 {
				sendStart, err := strconv.ParseInt(data[1].(map[string]interface{})["arguments"].([]interface{})[1].(string), 10, 64)
				if err != nil {
					s.logError(ctx, "Failed to decode start timestamp", err)
				} else {
					s.logLatency((time.Now().UnixNano() - sendStart) / 1000000)
				}
			}
		}
	}, func(_, newState string) {
		if newState == socketio.Connected {
			atomic.AddInt64(&s.connectionEstablished, 1)
			atomic.AddInt64(&s.connectionInProgress, -1)
		}
	})
	if err != nil {
		s.logError(ctx, "Failed to create socket.io client", err)
		return err
	}

	invocationID := 0
	// Send message
	sendMessage := func() error {
		invocation := socketio.Invocation{
			Target: "echo",
			Arg: map[string]interface{}{
				"type":         1,
				"invocationId": strconv.Itoa(invocationID),
				"arguments": []string{
					ctx.UserId,
					strconv.FormatInt(time.Now().UnixNano(), 10),
				},
				"nonBlocking": false,
			},
		}
		client.SendObjectMessage(42, invocation)
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
	atomic.AddInt64(&s.connectionEstablished, -1)

	return nil
}

func (s *SocketIOConnection) Counters() map[string]int64 {
	counters := map[string]int64{
		"socketio:connection:inprogress":      atomic.LoadInt64(&s.connectionInProgress),
		"socketio:connection:established":     atomic.LoadInt64(&s.connectionEstablished),
		"socketio:connection:error":           atomic.LoadInt64(&s.connectionError),
		"socketio:connection:closeerror":      atomic.LoadInt64(&s.connectionCloseError),
		"socketio:connection:success":         atomic.LoadInt64(&s.successCount),
		"socketio:connection:message:receive": atomic.LoadInt64(&s.messageReceiveCount),
		"socketio:connection:message:send":    atomic.LoadInt64(&s.messageSendCount),
		"socketio:connection:latency:lt_100":  atomic.LoadInt64(&s.latencyLessThan100ms),
		"socketio:connection:latency:lt_500":  atomic.LoadInt64(&s.latencyLessThan500ms),
		"socketio:connection:latency:lt_1000": atomic.LoadInt64(&s.latencyLessThan1000ms),
		"socketio:connection:latency:gt_1000": atomic.LoadInt64(&s.latencyGreaterThan1000ms),
	}

	for i := 0; i < maxInstances; i++ {
		if val := atomic.LoadInt64(&s.instanceHitCount[i]); val > 0 {
			counters["socketio:connection:instancehit:"+strconv.Itoa(i)] = val
		}
	}

	return counters
}
