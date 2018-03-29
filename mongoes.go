package eventstore

import (
	"fmt"
	"log"
	"time"

	"github.com/globalsign/mgo"
	"github.com/rwynn/gtm"
	"gopkg.in/mgo.v2/bson"
)

type MongoesConfig struct {
	MongoUri string `json:"mongouri"`
}

type mongoesEvent struct {
	Event      *Event
	SequenceID uint64
}

type EventStream = chan *Event

type mongoes struct {
	out              EventStream
	in               EventStream
	session          *mgo.Session
	lastPublishedSeq uint64
	ctx              *gtm.OpCtx
	cfg              *MongoesConfig
}

func (es *mongoes) Replay(lastEventID string) EventOutbound {
	out := make(EventStream)

	var replayStartSeq uint64
	col := es.session.DB("").C("events")
	if len(lastEventID) == 0 {
		replayStartSeq = 0
	} else {
		var e mongoesEvent
		err := col.Find(bson.M{"event.id": lastEventID}).One(&e)
		if err == nil {
			replayStartSeq = e.SequenceID
		}
	}
	afterStart := bson.M{"sequenceid": bson.M{"$gt": replayStartSeq}}
	includesEnd := bson.M{"sequenceid": bson.M{"$lte": es.lastPublishedSeq}}
	seqRange := bson.M{"$and": []bson.M{afterStart, includesEnd}}
	iter := col.Find(seqRange).Sort("sequenceid").Iter()

	go func() {
		var result mongoesEvent
		for iter.Next(&result) {
			out <- result.Event
		}
		iter.Close()
		close(out)
	}()
	return out
}

func (es *mongoes) nextSequenceId() uint64 {
	var result struct {
		Counter uint64
	}
	col := es.session.DB("").C("sequences")
	col.FindId("events").Apply(mgo.Change{
		Update:    bson.M{"$inc": bson.M{"counter": 1}},
		ReturnNew: true,
	}, &result)
	return result.Counter
}

func (es *mongoes) lastSequnceId() uint64 {
	var result struct {
		Counter uint64
	}
	es.session.DB("").C("sequences").FindId("events").One(&result)
	return result.Counter
}

func (es *mongoes) Start() error {
	if sess, err := mgo.Dial(es.cfg.MongoUri); err != nil {
		return fmt.Errorf("cannot connect to mongodb, uri: %s - %s", es.cfg.MongoUri, err.Error())
	} else {
		es.session = sess
	}

	es.session.SetMode(mgo.Monotonic, true)
	eventsCol := es.session.DB("").C("events")
	eventsCol.EnsureIndexKey("event.id")
	eventsCol.EnsureIndexKey("sequenceid")
	// only insert if not exists
	seqCol := es.session.DB("").C("sequences")
	seqCol.Insert(bson.M{"_id": "events", "counter": uint64(0)})
	es.lastPublishedSeq = es.lastSequnceId()

	seqNS := es.session.DB("").C("events").FullName
	es.ctx = gtm.Start(es.session, &gtm.Options{
		Filter: func(op *gtm.Op) bool {
			return op.Namespace == seqNS && op.IsInsert()
		},
	})

	sessionCheckTimer := time.NewTicker(time.Second)
	defer sessionCheckTimer.Stop()

	go func() {
		for {
			select {
			case e := <-es.in:
				es.session.DB("").C("events").Insert(mongoesEvent{
					Event:      e,
					SequenceID: es.nextSequenceId(),
				})

			case op := <-es.ctx.OpC:
				var e mongoesEvent
				if bytes, err := bson.Marshal(op.Data); err != nil {
					log.Printf("cannot serialize event: %s\n", err)
				} else {
					if err := bson.Unmarshal(bytes, &e); err != nil {
						log.Printf("cannot unserialize event: %s\n", err)
					}
				}

				es.lastPublishedSeq = e.SequenceID
				es.out <- e.Event

			case <-sessionCheckTimer.C:
				if err := es.session.Ping(); err != nil {
					log.Panic("mongo session lost")
				}
			}
		}
	}()
	return nil
}

func (es *mongoes) Stop() {
	if es.session != nil {
		es.session.Close()
	}
	if es.ctx != nil {
		es.ctx.Stop()
	}
}

func NewMongoes(conf *MongoesConfig) EventStore {
	return &mongoes{
		in:  make(EventStream),
		out: make(EventStream),
		cfg: conf,
	}
}

func (b *mongoes) Add() EventInbound {
	return b.in
}

func (b *mongoes) Added() EventOutbound {
	return b.in
}
