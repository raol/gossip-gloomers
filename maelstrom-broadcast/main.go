package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sort"
	"sync"
	"time"
)

type state struct {
	mu       sync.Mutex
	values   []float64
	topology []string
}

type gossipState struct {
	mu          sync.Mutex
	outstanding map[string]map[float64]bool
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	s := &state{
		values:   make([]float64, 0),
		topology: make([]string, 0),
	}

	gs := &gossipState{
		outstanding: make(map[string]map[float64]bool),
	}

	node := maelstrom.NewNode()

	timer := time.NewTicker(100 * time.Millisecond)
	go func() {
		for {
			select {
			case <-timer.C:
				gs.mu.Lock()
				for k, _ := range gs.outstanding {
					go nodeGossip(node, k, gs)
				}
				gs.mu.Unlock()
			}
		}
	}()

	node.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mu.Lock()
		gs.mu.Lock()

		defer s.mu.Unlock()
		defer gs.mu.Unlock()

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if gossip, ok := body["message"].([]any); ok {
			for _, e := range gossip {
				value := e.(float64)
				if !contains(s.values, value) {
					s.values = append(s.values, value)
					for _, v := range gs.outstanding {
						v[value] = false
					}
				}
			}
			sort.Float64s(s.values)
		} else {
			log.Printf("Failed to parse gossip body")
		}

		return node.Reply(msg, map[string]any{
			"type": "gossip_ok",
		})
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		s.mu.Lock()
		gs.mu.Lock()
		defer s.mu.Unlock()
		defer gs.mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if len(s.topology) == 0 {
			s.topology = node.NodeIDs()
		}

		value := body["message"].(float64)
		if contains(s.values, value) {
			return nil
		}

		s.values = append(s.values, value)
		sort.Float64s(s.values)
		for _, v := range gs.outstanding {
			v[value] = false
		}

		return node.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["messages"] = s.values

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var topology []any
		if top, ok := body["topology"].(map[string]any); ok {
			if topology, ok = top[node.ID()].([]any); ok {
				for _, t := range topology {
					s.topology = append(s.topology, t.(string))
				}
			} else {
				s.topology = node.NodeIDs()
			}
		}

		gs.mu.Lock()
		for _, n := range s.topology {
			gs.outstanding[n] = make(map[float64]bool)
		}
		defer gs.mu.Unlock()

		return node.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func nodeGossip(node *maelstrom.Node, dest string, state *gossipState) {
	state.mu.Lock()
	values := make([]float64, 0)

	for k, v := range state.outstanding[dest] {
		if !v {
			values = append(values, k)
		}
	}

	state.mu.Unlock()

	if len(values) == 0 {
		return
	}

	log.Printf("Values to be sent to %s are %v", dest, values)

	body := map[string]any{
		"type":    "gossip",
		"message": values,
	}

	reply := make(chan any)
	go func() {
		node.RPC(dest, body, func(msg maelstrom.Message) error {
			reply <- true
			return nil
		})
	}()

	select {
	case <-reply:
		state.mu.Lock()
		for _, e := range values {
			state.outstanding[dest][e] = true
		}
		state.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		log.Printf("Sending to %s timed out", dest)
		return
	}
}

func contains[A comparable](collection []A, element A) bool {
	for _, e := range collection {
		if e == element {
			return true
		}
	}

	return false
}
