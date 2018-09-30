package queue_redis_test

import (
	"testing"
	"time"

	"github.com/carltd/glib/queue"
	"github.com/carltd/glib/queue/message"
	_ "github.com/carltd/glib/queue/queue_redis"
	"github.com/carltd/glib/queue/testdata"
	"github.com/carltd/glib/queue/util"
)

const (
	driverName  = "redis"
	redisDSN    = "redis://:123456@127.0.0.1:6379/0?maxIdle=10&maxActive=10&idleTimeout=3"
	testSubject = "testSubject"
)

func newSubscriber() (queue.Subscriber, error) {
	qc, err := queue.NewConsumer(driverName, redisDSN)
	if err != nil {
		return nil, err
	}
	return qc.Subscribe(testSubject, "test")
}

func TestNewPublisher(t *testing.T) {

	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	msg := &message.Message{
		Priority: message.MsgPriority_PRIORITY0,
		Body:     util.MustMessageBody(want),
	}

	ch := make(chan struct{})
	go func() {
		sub, err := newSubscriber()
		if err != nil {
			t.Error(err)
		}
		ch <- struct{}{}
		m, err := sub.NextMessage(10 * time.Second)
		if err != nil {
			t.Error(err)
		}
		got := testdata.Something{}
		if err := util.FromMessageBody(m.Body, &got); err != nil {
			t.Error(err)
		}
		if msg.MessageId != m.MessageId {
			t.Errorf("message id: want %#x, got %#x", msg.MessageId, m.MessageId)
		}

		if want.Name != got.Name {
			t.Errorf("name: want %v, got %v", want.Name, got.Name)
		}

		if want.Age != got.Age {
			t.Errorf("Age: want %v, got %v", want.Age, got.Age)
		}

		ch <- struct{}{}
	}()

	<-ch
	qc, _ := queue.NewPublisher(driverName, redisDSN)
	if err := qc.Publish(testSubject, msg); err != nil {
		t.Error(err)
	}
	qc.Close()
	<-ch
}

func TestDrivers(t *testing.T) {
	ds := queue.Drivers()
	if len(ds) != 1 {
		t.Errorf("driver want 1, got %d", len(ds))
	}
	if ds[0] != driverName {
		t.Errorf("driver's name want %s, got %s", driverName, ds[0])
	}
}

func TestRedisQueueConn_Enqueue(t *testing.T) {
	qc, _ := queue.NewPublisher(driverName, redisDSN)

	defer qc.Close()
	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	msg := &message.Message{
		Priority: message.MsgPriority_PRIORITY0,
		Body:     util.MustMessageBody(want),
	}
	if err := qc.Enqueue(testSubject, msg); err != nil {
		t.Error(err)
	}
}

func TestRedisQueueConn_Dequeue(t *testing.T) {

	TestRedisQueueConn_Enqueue(t)

	qc, _ := queue.NewConsumer(driverName, redisDSN)

	defer qc.Close()
	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	got := testdata.Something{}
	_, err := qc.Dequeue(testSubject, "test", 10*time.Second, &got)
	if err != nil {
		t.Fatal(err)
	}

	if want.Name != got.Name {
		t.Errorf("want %s, got %s", want.Name, got.Name)
	}
	if want.Age != got.Age {
		t.Errorf("want %v, got %v", want.Age, got.Age)
	}
}
