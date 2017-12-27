package sigbench

import (
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"microsoft.com/sigbench/snapshot"
)

type MasterController struct {
	Agents         []*AgentDelegate
	SnapshotWriter snapshot.SnapshotWriter
}

func (c *MasterController) RegisterAgent(address string) error {
	if agentDelegate, err := NewAgentDelegate(address); err == nil {
		c.Agents = append(c.Agents, agentDelegate)
		return nil
	} else {
		return err
	}
}

func (c *MasterController) setupAllAgents(job *Job) error {
	var wg sync.WaitGroup

	for _, agent := range c.Agents {
		wg.Add(1)
		go func(agent *AgentDelegate) {
			args := &AgentSetupArgs{
				SessionParams: job.SessionParams,
			}
			var result AgentSetupResult
			if err := agent.Client.Call("AgentController.Setup", args, &result); err != nil {
				// TODO: Report error
				log.Fatalln(err)
			}
			wg.Done()
		}(agent)
	}

	wg.Wait()
	return nil
}

func (c *MasterController) collectCounters(sessionNames []string) map[string]int64 {
	counters := make(map[string]int64)
	for _, agent := range c.Agents {
		args := &AgentListCountersArgs{
			SessionNames: sessionNames,
		}
		var result AgentListCountersResult
		if err := agent.Client.Call("AgentController.ListCounters", args, &result); err != nil {
			log.Println("ERROR: Fail to list counters from agent:", agent.Address, err)
		}
		for k, v := range result.Counters {
			counters[k] = counters[k] + v
		}
	}
	return counters
}

func (c *MasterController) watchCounters(sessionNames []string, stopChan chan struct{}) {
	secondsPassed := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			counters := c.collectCounters(sessionNames)

			if err := c.SnapshotWriter.WriteCounters(time.Now(), counters); err != nil {
				log.Println("Error: fail to write counter snapshot: ", err)
			}

			c.printCounters(counters)
			secondsPassed++
			if secondsPassed >= 150 {
				log.Println("*** 150 seconds passed ***")
			} else if secondsPassed >= 100 {
				log.Println(">>> 100 seconds passed <<<")
			}
		case <-stopChan:
			return
		}
	}
}

func (c *MasterController) printCounters(counters map[string]int64) {
	table := make([][2]string, 0, len(counters))
	for k, v := range counters {
		table = append(table, [2]string{k, strconv.FormatInt(v, 10)})
	}

	sort.Slice(table, func(i, j int) bool {
		return table[i][0] < table[j][0]
	})

	log.Println("Counters:")
	for _, row := range table {
		log.Println("    ", row[0], ": ", row[1])
	}
}

func (c *MasterController) Run(job *Job) error {
	// TODO: Validate job
	var wg sync.WaitGroup
	var agentCount int = len(c.Agents)
	var timeStart time.Time = time.Now()

	if err := c.setupAllAgents(job); err != nil {
		return err
	}

	for idx, agent := range c.Agents {
		wg.Add(1)
		go func(idx int, agent *AgentDelegate) {
			args := &AgentRunArgs{
				Job:        *job,
				AgentCount: agentCount,
				AgentIdx:   idx,
			}
			var result AgentRunResult
			if err := agent.Client.Call("AgentController.Run", args, &result); err != nil {
				// TODO: report error
				log.Println(err)
			}

			wg.Done()
		}(idx, agent)
	}

	stopWatchCounterChan := make(chan struct{})
	go c.watchCounters(job.SessionNames, stopWatchCounterChan)

	wg.Wait()

	close(stopWatchCounterChan)

	log.Println("--- Finished ---")
	counters := c.collectCounters(job.SessionNames)
	c.SnapshotWriter.WriteCounters(time.Now(), counters)
	c.printCounters(counters)

	totalDuration := int64(time.Now().Sub(timeStart) / time.Second)
	log.Println("Test duration:", totalDuration, "secs")

	return nil
}
