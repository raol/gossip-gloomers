package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
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
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	timer := time.NewTicker(100 * time.Millisecond)
	go func() {
		for {
			select {
			case <-timer.C:
				go nodeGossip(node, kv)
			}
		}
	}()

	node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		value, _ := kv.ReadInt(context.Background(), node.ID())

		kv.Write(context.Background(), node.ID(), value+delta)

		return node.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		value := 0
		for _, nodeId := range node.NodeIDs() {
			if v, err := kv.ReadInt(context.Background(), nodeId); err == nil {
				value += v
			}
		}
		return node.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
		})
	})

	node.Handle("gossip", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := int(body["value"].(float64))
		kv.Write(context.Background(), msg.Src, value)

		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}

func nodeGossip(node *maelstrom.Node, kv *maelstrom.KV) {
	if value, err := kv.ReadInt(context.Background(), node.ID()); err == nil {
		for _, id := range node.NodeIDs() {
			if id == node.ID() {
				continue
			}

			node.Send(id, map[string]any{
				"type":  "gossip",
				"value": value,
			})
		}
	}
}
