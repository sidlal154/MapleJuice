package queue

import "fmt"

type MJQueue struct {
	items []MJQueueEntry
}

type MJQueueEntry struct {
	Operation   string //[MAPLE,JUICE]
	Executable  string //[exe file along with args]
	WorkerCount int    //[numJuices/numMaples]
	InputFile   string //[sdfsSrcDir/sdfsDestName]
	Prefix      string //[interFilePrefix]
	Delete      bool   //[Delete={0,1}, Dont care for MAPLE]
}

func (q *MJQueue) Enqueue(item MJQueueEntry) {
	q.items = append(q.items, item)
}

func (q *MJQueue) Dequeue() MJQueueEntry {
	if len(q.items) == 0 {
		fmt.Println("Queue is empty")
		return MJQueueEntry{}
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item
}

func (q *MJQueue) IsEmpty() bool {
	return len(q.items) == 0
}
