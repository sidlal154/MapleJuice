package node

import "fmt"

type MJQueue struct {
	items []MJQueueEntry
}

func NewMJQueue() *MJQueue {
	return &MJQueue{}
}

type MJQueueEntry struct {
	operation   string
	executable  string
	workerCount string
	inputFile   string
	prefix      string
	target      string
	regexp      string
	finalColumn string
	delete      int
}

func (q *MJQueue) Enqueue(item MJQueueEntry) {
	q.items = append(q.items, item)
}

func (q *MJQueue) Dequeue() {
	if len(q.items) == 0 {
		fmt.Println("Queue is empty")
		return
	}
	q.items = q.items[1:]
}

func (q *MJQueue) Peek() MJQueueEntry {
	if len(q.items) == 0 {
		return MJQueueEntry{}
	}
	return q.items[0]
}
