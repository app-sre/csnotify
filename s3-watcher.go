package csnotify

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Event represents an object notification.
type Event struct {
	Path string // path to object.
}

// newEvent returns a new Event.
func newEvent(path string) Event {
	e := Event{Path: path}
	return e
}

// Watcher watches a set of objects, delivering events to a channel.
type Watcher struct {
	Events   chan Event
	Errors   chan error
	svc      *s3.S3
	paths    map[string]string // List of watched paths
	done     chan struct{}     // Channel for sending a "quit message" to the reader goroutine
	doneResp chan struct{}     // Channel to respond to Close
}

// NewWatcher creates a new session and begins waiting for events.
func NewWatcher() (*Watcher, error) {
	svc := s3.New(session.New())
	w := &Watcher{
		svc:      svc,
		paths:    make(map[string]string),
		Events:   make(chan Event),
		Errors:   make(chan error),
		done:     make(chan struct{}),
		doneResp: make(chan struct{}),
	}

	go w.readEvents()
	return w, nil
}

// Add starts watching the specified S3 object path (non-recursively).
func (w *Watcher) Add(path string) {
	// Initial value to be different from any ETag
	w.paths[path] = "Ooh, watch me, watch me!"
}

// Remove stops watching the specified S3 object path (non-recursively).
func (w *Watcher) Remove(path string) {
	delete(w.paths, path)
}

// readEvents gets changes of the S3 object, converts the
// results into Event objects and sends them via the Events channel
func (w *Watcher) readEvents() {
	defer close(w.doneResp)
	defer close(w.Errors)
	defer close(w.Events)

	for {
		for path, eTag := range w.paths {
			cleanPath := strings.Split(path, "s3://")[1]
			pSplit := strings.Split(cleanPath, "/")
			bucket, key := pSplit[0], strings.Join(pSplit[1:], "/")

			input := &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			}
			result, err := w.svc.HeadObject(input)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					select {
					case w.Errors <- aerr:
					case <-w.done:
						return
					}
				} else {
					select {
					case w.Errors <- err:
					case <-w.done:
						return
					}
				}
			}
			currentETag := *result.ETag
			if currentETag != eTag {
				event := newEvent(path)
				w.paths[path] = currentETag
				select {
				case w.Events <- event:
				case <-w.done:
					return
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
}
