package node

import "fmt"

type Queue struct {
	front *QueueEntry
	rear  *QueueEntry
}

type QueueEntry struct {
	prev      *QueueEntry
	operation string
	machineId string
	next      *QueueEntry
}

func NewQueue() *Queue {
	return &Queue{}
}

func (q *Queue) Enqueue(operation string, machineId string) {
	newNode := &QueueEntry{operation: operation, machineId: machineId}

	if q.rear == nil || q.front == nil{
		q.front = newNode
		q.rear = newNode
	} else {
		length := 0
		entry := q.rear
		for entry != nil {
			length++
			entry = entry.prev
		}
		if length <= 4 || q.rear.operation == newNode.operation {
			newNode.prev = q.rear
			q.rear.next = newNode
			q.rear = newNode
		} else {
			found := false
			entry = q.rear
			count := 0
			for entry != nil && count < 4 {
				// fmt.Println(entry.machineId)
				count++
				if entry.operation == newNode.operation {
					// fmt.Println("Same operation:" + entry.machineId)
					found = true
					break
				}
				entry = entry.prev
			}
			// fmt.Println("After Loop: " + entry.machineId + ":" + entry.operation)
			// fmt.Println("After Loop: ", count)
			if found || entry.operation == newNode.operation {
				newNode.prev = q.rear
				q.rear.next = newNode
				q.rear = newNode
			} else {
				for entry.prev != nil || entry.operation != newNode.operation {
					if entry.operation == newNode.operation {
						break
					}
					count++
					// fmt.Println(entry.machineId + ":" + entry.operation)
					entry = entry.prev
				}
				iter := 0
				// fmt.Println("Terminal", entry.machineId+":"+entry.operation)
				for {
					if iter == 4 {
						break
					}
					iter++
					entry = entry.next
					// fmt.Println("BACKTRACK", entry.machineId+":"+entry.operation)
				}
				nextEntry := entry.next
				// fmt.Println("ENTER insertion")
				// fmt.Println("REAR:" + nextEntry.machineId + ":" + nextEntry.operation)
				// fmt.Println("newNode:" + newNode.machineId + ":" + newNode.operation)
				newNode.prev = nextEntry.prev
				// fmt.Println("newNode.PREV:" + newNode.prev.machineId + ":" + newNode.prev.operation)
				// q.rear.prev = newNode
				nextEntry.prev = newNode
				newNode.prev.next = newNode
				// fmt.Println("nextEntry.PREV:" + nextEntry.prev.machineId + ":" + nextEntry.prev.operation)
				newNode.next = nextEntry
				// fmt.Println("newNode.NEXT:" + newNode.next.machineId + ":" + newNode.next.operation)
				// q.rear.next = nil
			}
		}

		// if q.rear.operation != newNode.operation{
		// 	entry := q.rear
		// 	count := 1
		// 	for entry.prev != nil && entry.operation == q.rear.operation{
		// 		if count == 4
		// 		entry = entry.prev
		// 		count ++
		// 	}
		// }
		// newNode.prev = q.front
		// q.rear.next = newNode
		// q.rear = newNode
	}
}

func (q *Queue) Dequeue() string {
	if q.front == nil {
		return ""
	}

	machineId := q.front.machineId
	q.front = q.front.next

	if q.front == nil {
		q.rear = nil
	}

	return machineId
}

func (q *Queue) IsEmpty() bool {
	return q.front == nil
}

func (q *Queue) Peek() string {
	if q.front == nil {
		return "NONE"
	}
	return q.front.machineId
}

func (q *Queue) Peek2nd() string {
	if q.front == nil || q.front.next == nil{
		return "NONE"
	}
	return q.front.next.machineId
}

func (q *Queue) DequeueID(machineId string) {
	if q.front == nil {
		return
	}
	entry := q.front
	for entry.next != nil {
		if entry.machineId == machineId {
			fmt.Println("machineId:", entry.machineId)
			break
		}
		entry = entry.next
	}
	fmt.Println(entry.machineId)
	if entry == q.front {
		fmt.Println("YES")
		if q.front == q.rear {
			q.front = nil
			q.rear = nil
			fmt.Println("Only 1 element in queue")
		} else {
			q.front = q.front.next
		}
		
		
	} else if entry == q.rear {
		fmt.Println("LAST")
		q.rear = q.rear.prev
		q.rear.next = nil
	} else {
		fmt.Println("ENTRY:", entry.machineId)
		fmt.Println("Entry.prev:", entry.prev.machineId)
		fmt.Println("Entry.next:", entry.next.machineId)
		entry.prev.next = entry.next
		entry.next.prev = entry.prev
	}
}

func (q *Queue) DequeueAll(machineId string) {
	entry := q.front
	for entry != nil {
		if entry.machineId == machineId {
			if entry == q.front {
				q.front = q.front.next
				q.front.prev.next = nil
				entry = q.front
			} else if entry == q.rear {
				q.rear = entry.prev
				q.rear.next = nil
				break
			} else {
				newNode := entry.next
				newNode.prev = entry.prev
				newNode.prev.next = newNode
				entry.prev = nil
				entry.next = nil
				entry = newNode.prev
			}
		}
		entry = entry.next
	}
}
