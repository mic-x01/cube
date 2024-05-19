package store

import (
	"cube/task"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

type Store interface {
	Put(key string, value interface{}) error
	Get(key string) (interface{}, error)
	List() (interface{}, error)
	Count() (int, error)
}

type InMemoryTaskStore struct {
	Db map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		Db: make(map[string]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key string, value interface{}) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("value %v is not a task.Task type", value)
	}
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key string) (interface{}, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist", key)
	}

	return t, nil
}

func (i *InMemoryTaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

type InMemoryTaskEventStore struct {
	Db map[string]*task.TaskEvent
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		Db: make(map[string]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, value interface{}) error {
	e, ok := value.(*task.TaskEvent)
	if !ok {
		return fmt.Errorf("value %v is not a task.TaskEvent type", value)
	}
	i.Db[key] = e
	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (interface{}, error) {
	e, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task event with key %s does not exist", key)
	}

	return e, nil
}

func (i *InMemoryTaskEventStore) List() (interface{}, error) {
	var events []*task.TaskEvent
	for _, e := range i.Db {
		events = append(events, e)
	}
	return events, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}

type TaskStore struct {
	Db       *badger.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func NewTaskStore(file string, mode os.FileMode, bucket string) (*TaskStore, error) {
	db, err := badger.Open(badger.DefaultOptions(file))
	if err != nil {
		return nil, fmt.Errorf("unable to open %v", file)
	}

	t := TaskStore{
		DbFile: file,
		Db:     db,
		Bucket: bucket,
	}

	return &t, nil
}

func (t *TaskStore) Close() {
	t.Db.Close()
}

func (t *TaskStore) Count() (int, error) {
	taskCount := 0

	err := t.Db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(t.Bucket)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			taskCount++
		}
		return nil
	})

	if err != nil {
		return -1, err
	}

	return taskCount, nil
}

func (t *TaskStore) Put(key string, value interface{}) error {
	return t.Db.Update(func(txn *badger.Txn) error {
		buf, err := json.Marshal(value.(*task.Task))
		if err != nil {
			return err
		}
		var keyWithBucket strings.Builder
		keyWithBucket.WriteString(t.Bucket)
		keyWithBucket.WriteString("-")
		keyWithBucket.WriteString(key)
		err = txn.Set([]byte(keyWithBucket.String()), buf)
		if err != nil {
			return err
		}

		return nil
	})
}

func (t *TaskStore) Get(key string) (interface{}, error) {
	var task task.Task
	err := t.Db.View(func(txn *badger.Txn) error {
		var keyWithBucket strings.Builder
		keyWithBucket.WriteString(t.Bucket)
		keyWithBucket.WriteString("-")
		keyWithBucket.WriteString(key)
		item, err := txn.Get([]byte(keyWithBucket.String()))
		if err != nil {
			return fmt.Errorf("task %v not found", key)
		}
		err = item.Value(func(val []byte) error {

			err = json.Unmarshal(val, &task)
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (t *TaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	err := t.Db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(t.Bucket)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {
				var task task.Task
				err := json.Unmarshal(v, &task)
				if err != nil {
					return err
				}
				tasks = append(tasks, &task)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return tasks, nil
}

type EventStore struct {
	DbFile string
	Db     *badger.DB
	Bucket string
}

func NewEventStore(file string, mode os.FileMode, bucket string) (*EventStore, error) {
	db, err := badger.Open(badger.DefaultOptions(file))
	if err != nil {
		return nil, fmt.Errorf("unable to open %v", file)
	}

	e := EventStore{
		DbFile: file,
		Db:     db,
		Bucket: bucket,
	}

	return &e, nil
}

func (e *EventStore) Close() {
	e.Db.Close()
}

func (e *EventStore) Count() (int, error) {
	eventCount := 0

	err := e.Db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(e.Bucket)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			eventCount++
		}
		return nil
	})

	if err != nil {
		return -1, err
	}

	return eventCount, nil
}

func (e *EventStore) Put(key string, value interface{}) error {
	return e.Db.Update(func(txn *badger.Txn) error {
		buf, err := json.Marshal(value.(*task.TaskEvent))
		if err != nil {
			return err
		}
		var keyWithBucket strings.Builder
		keyWithBucket.WriteString(e.Bucket)
		keyWithBucket.WriteString("-")
		keyWithBucket.WriteString(key)
		err = txn.Set([]byte(keyWithBucket.String()), buf)
		if err != nil {
			return err
		}

		return nil
	})
}

func (e *EventStore) Get(key string) (interface{}, error) {
	var event task.TaskEvent
	err := e.Db.View(func(txn *badger.Txn) error {
		var keyWithBucket strings.Builder
		keyWithBucket.WriteString(e.Bucket)
		keyWithBucket.WriteString("-")
		keyWithBucket.WriteString(key)
		item, err := txn.Get([]byte(keyWithBucket.String()))
		if err != nil {
			return fmt.Errorf("task %v not found", key)
		}
		err = item.Value(func(val []byte) error {

			err = json.Unmarshal(val, &event)
			if err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &event, nil
}

func (e *EventStore) List() (interface{}, error) {
	var events []*task.TaskEvent
	err := e.Db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(e.Bucket)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(v []byte) error {
				var event task.TaskEvent
				err := json.Unmarshal(v, &event)
				if err != nil {
					return err
				}
				events = append(events, &event)
				return nil
			})

			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return events, nil
}
