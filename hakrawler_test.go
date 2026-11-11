package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testWait = 5 * time.Second

func seedLines(base string, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%s/seed/%d\n", base, i)
	}
	return b.String()
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testWait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitInt(t *testing.T, ch <-chan int, what string) int {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testWait):
		t.Fatalf("timed out waiting for %s", what)
	}
	return 0
}

// onceCloser returns an idempotent closer so deferred failure cleanup and the
// normal path can both release gated handlers.
func onceCloser(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func runInBackground(t *testing.T, args []string, stdin io.Reader, stdout io.Writer) (<-chan int, *bytes.Buffer) {
	t.Helper()
	stderr := &bytes.Buffer{}
	done := make(chan int, 1)
	go func() {
		done <- run(args, stdin, stdout, stderr)
	}()
	return done, stderr
}

func waitDone(t *testing.T, done <-chan int) int {
	t.Helper()
	select {
	case code := <-done:
		return code
	case <-time.After(testWait):
		t.Fatal("crawler did not finish")
	}
	return -1
}

// TestSeedConcurrencyCapAndOrder proves that no more than -seed-concurrency
// seeds are active at once, that seeds are admitted in stdin order, and that
// blocking seed responses gate the demonstration (no sleeps or public net).
func TestSeedConcurrencyCapAndOrder(t *testing.T) {
	const concurrency = 2
	arrivals := make(chan int, 16)
	release := make(chan struct{}, 8)
	var active, maxActive int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		idx, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/seed/"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		cur := atomic.AddInt32(&active, 1)
		for {
			max := atomic.LoadInt32(&maxActive)
			if cur <= max || atomic.CompareAndSwapInt32(&maxActive, max, cur) {
				break
			}
		}
		arrivals <- idx
		select {
		case <-release:
		case <-r.Context().Done():
		}
		atomic.AddInt32(&active, -1)
		fmt.Fprintf(w, "<html><body>seed %d</body></html>", idx)
	}))
	defer srv.Close()
	defer onceCloser(release)()

	done, stderr := runInBackground(t, []string{"-seed-concurrency", strconv.Itoa(concurrency), "-t", "8"},
		strings.NewReader(seedLines(srv.URL, 4)), io.Discard)

	// The first two admitted seeds must be the first two stdin lines. Their
	// HTTP arrivals may race with each other, so compare the set.
	first := waitInt(t, arrivals, "first seed")
	second := waitInt(t, arrivals, "second seed")
	if (first != 0 || second != 1) && (first != 1 || second != 0) {
		t.Fatalf("first batch = {%d,%d}, want {0,1}", first, second)
	}
	if atomic.LoadInt32(&maxActive) != concurrency {
		t.Fatalf("active seeds = %d, want %d", maxActive, concurrency)
	}

	// With both workers blocked there must be no third activation yet.
	select {
	case idx := <-arrivals:
		t.Fatalf("seed %d activated while both slots were occupied", idx)
	case <-time.After(250 * time.Millisecond):
	}

	// Free exactly one slot: the next admitted line must be 2, and only then
	// (with the other original still blocked) must line 3 be admitted.
	release <- struct{}{}
	if got := waitInt(t, arrivals, "third seed"); got != 2 {
		t.Fatalf("third activated seed = %d, want 2", got)
	}
	release <- struct{}{}
	if got := waitInt(t, arrivals, "fourth seed"); got != 3 {
		t.Fatalf("fourth activated seed = %d, want 3", got)
	}
	release <- struct{}{}
	release <- struct{}{}

	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := atomic.LoadInt32(&maxActive); got > concurrency {
		t.Fatalf("maximum active seeds = %d, want <= %d", got, concurrency)
	}
}

// TestSlowSeedDoesNotBlockFreeSlots proves one gated/slow seed only occupies
// its own worker slot; other seeds keep being admitted and crawled.
func TestSlowSeedDoesNotBlockFreeSlots(t *testing.T) {
	slowArrived := make(chan struct{}, 1)
	freeArrived := make(chan int, 8)
	releaseSlow := make(chan struct{})
	closeSlow := onceCloser(releaseSlow)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/slow":
			slowArrived <- struct{}{}
			select {
			case <-releaseSlow:
			case <-r.Context().Done():
			}
			io.WriteString(w, "<html><body>slow</body></html>")
		case strings.HasPrefix(r.URL.Path, "/fast/"):
			idx, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/fast/"))
			freeArrived <- idx
			io.WriteString(w, "<html><body>fast</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	defer closeSlow()

	stdin := strings.NewReader(fmt.Sprintf("%s/slow\n%s/fast/1\n%s/fast/2\n", srv.URL, srv.URL, srv.URL))
	done, stderr := runInBackground(t, []string{"-seed-concurrency", "2", "-t", "4"}, stdin, io.Discard)

	waitClosed(t, slowArrived, "slow seed request")

	// The other worker must crawl both remaining seeds while the slow seed
	// is still blocked in its handler.
	if got := waitInt(t, freeArrived, "first fast seed"); got != 1 {
		t.Fatalf("fast seed index = %d, want 1", got)
	}
	if got := waitInt(t, freeArrived, "second fast seed"); got != 2 {
		t.Fatalf("fast seed index = %d, want 2", got)
	}

	closeSlow()
	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
}

// TestProcessWideRequestCap proves -t bounds incomplete HTTP requests across
// all seed collectors, not per collector.
func TestProcessWideRequestCap(t *testing.T) {
	const limit = 2
	arrivals := make(chan struct{}, 32)
	release := make(chan struct{}, 32)
	var inFlight, maxInFlight int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			max := atomic.LoadInt32(&maxInFlight)
			if cur <= max || atomic.CompareAndSwapInt32(&maxInFlight, max, cur) {
				break
			}
		}
		arrivals <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		atomic.AddInt32(&inFlight, -1)
		io.WriteString(w, "<html><body>page</body></html>")
	}))
	defer srv.Close()
	defer onceCloser(release)()

	done, stderr := runInBackground(t, []string{"-seed-concurrency", "4", "-t", strconv.Itoa(limit)},
		strings.NewReader(seedLines(srv.URL, 6)), io.Discard)

	for i := 0; i < 6; i += limit {
		waitClosed(t, arrivals, fmt.Sprintf("request %d", i))
		waitClosed(t, arrivals, fmt.Sprintf("request %d", i+1))

		// No third request may start while two are incomplete.
		select {
		case <-arrivals:
			t.Fatalf("more than %d incomplete HTTP requests", limit)
		case <-time.After(250 * time.Millisecond):
		}

		release <- struct{}{}
		release <- struct{}{}
	}

	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := atomic.LoadInt32(&maxInFlight); got > limit {
		t.Fatalf("maximum incomplete HTTP requests = %d, want <= %d", got, limit)
	}
}

// TestRedirectHopsCountButDoNotDeadlock proves redirect hops pass through the
// shared limiter and that a redirect does not hold its slot across the next
// hop (which would deadlock with -t 2 and two redirecting seeds).
func TestRedirectHopsCountButDoNotDeadlock(t *testing.T) {
	const limit = 2
	finals := make(chan int, 4)
	release := make(chan struct{}, 4)
	var inFlight, maxInFlight int32
	countInFlight := func(delta int32) {
		cur := atomic.AddInt32(&inFlight, delta)
		if delta > 0 {
			for {
				max := atomic.LoadInt32(&maxInFlight)
				if cur <= max || atomic.CompareAndSwapInt32(&maxInFlight, max, cur) {
					break
				}
			}
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case strings.HasPrefix(r.URL.Path, "/redir/"):
			countInFlight(1)
			defer countInFlight(-1)
			idx := strings.TrimPrefix(r.URL.Path, "/redir/")
			http.Redirect(w, r, "/final/"+idx, http.StatusFound)
		case strings.HasPrefix(r.URL.Path, "/final/"):
			idx, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/final/"))
			countInFlight(1)
			finals <- idx
			select {
			case <-release:
			case <-r.Context().Done():
			}
			countInFlight(-1)
			io.WriteString(w, "<html><body>final</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	defer onceCloser(release)()

	stdin := strings.NewReader(fmt.Sprintf("%s/redir/0\n%s/redir/1\n", srv.URL, srv.URL))
	done, stderr := runInBackground(t, []string{"-seed-concurrency", "2", "-t", strconv.Itoa(limit)}, stdin, io.Discard)

	// If the first redirect hop held its token until the final response, these
	// would never arrive with both seeds active and -t 2. Concurrent seeds may
	// finish in either order, so check the set rather than a fixed order.
	seen := map[int]bool{}
	seen[waitInt(t, finals, "first redirect target")] = true
	seen[waitInt(t, finals, "second redirect target")] = true
	if !seen[0] || !seen[1] {
		t.Fatalf("redirect targets = %v, want {0,1}", seen)
	}

	select {
	case <-finals:
		t.Fatalf("more than %d requests in flight during redirects", limit)
	case <-time.After(250 * time.Millisecond):
	}

	release <- struct{}{}
	release <- struct{}{}
	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := atomic.LoadInt32(&maxInFlight); got > limit {
		t.Fatalf("maximum in-flight requests = %d, want <= %d", got, limit)
	}
}

// TestTimeoutCancelsSeedAndReleasesSlot proves -timeout starts at activation,
// cancels the hanging request (gated server sees disconnect), emits no late
// output or links, and frees the only worker slot for the next seed.
func TestTimeoutCancelsSeedAndReleasesSlot(t *testing.T) {
	hangArrived := make(chan struct{}, 1)
	hangCanceled := make(chan struct{}, 1)
	freeArrived := make(chan struct{}, 1)
	releaseHang := make(chan struct{})
	lateLinks := int32(0)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case "/hang":
			hangArrived <- struct{}{}
			select {
			case <-r.Context().Done():
				hangCanceled <- struct{}{}
				// This response is written after the client has given up. It
				// must not cause a late link visit or output.
				io.WriteString(w, `<html><body><a href="/late">late</a></body></html>`)
			case <-releaseHang:
			}
		case "/fast":
			freeArrived <- struct{}{}
			io.WriteString(w, `<html><body><a href="/fastlink">fastlink</a></body></html>`)
		case "/fastlink":
			io.WriteString(w, "<html><body>ok</body></html>")
		case "/late":
			atomic.AddInt32(&lateLinks, 1)
			io.WriteString(w, "<html><body>late</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	defer onceCloser(releaseHang)()

	stdin := strings.NewReader(fmt.Sprintf("%s/hang\n%s/fast\n", srv.URL, srv.URL))
	stdout := &bytes.Buffer{}
	done, stderr := runInBackground(t, []string{"-seed-concurrency", "1", "-t", "2", "-timeout", "1"}, stdin, stdout)

	waitClosed(t, hangArrived, "hanging seed request")
	waitClosed(t, hangCanceled, "server-side cancellation of hanging request")
	waitClosed(t, freeArrived, "next seed after timeout")

	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "[timeout]") {
		t.Fatalf("timeout was not reported on stderr: %s", stderr.String())
	}
	if got := atomic.LoadInt32(&lateLinks); got != 0 {
		t.Fatalf("late link was visited %d times after timeout", got)
	}
	out := stdout.String()
	if !strings.Contains(out, "/fastlink") {
		t.Fatalf("output from following seed missing: %q", out)
	}
	if strings.Contains(out, "/late") {
		t.Fatalf("late output emitted after timeout: %q", out)
	}
}

// gatedWriter blocks the first write until the test releases it, providing a
// deterministic slow output consumer.
type gatedWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	buf     bytes.Buffer
}

func newGatedWriter() *gatedWriter {
	return &gatedWriter{started: make(chan struct{}), release: make(chan struct{})}
}

func (g *gatedWriter) Write(p []byte) (int, error) {
	g.once.Do(func() { close(g.started) })
	<-g.release
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.Write(p)
}

func (g *gatedWriter) String() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.String()
}

// TestSlowOutputConsumerBackpressure proves the output channel is bounded:
// when stdout stops consuming, HTTP work stops advancing, and it resumes once
// the consumer drains. No timing-based sleeps are used to hold the crawler.
func TestSlowOutputConsumerBackpressure(t *testing.T) {
	const links = 1000
	var leafRequests int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/root":
			fmt.Fprint(w, "<html><body>")
			for i := 0; i < links; i++ {
				fmt.Fprintf(w, `<a href="/leaf/%d">x</a>`, i)
			}
			fmt.Fprint(w, "</body></html>")
		case strings.HasPrefix(r.URL.Path, "/leaf/"):
			atomic.AddInt32(&leafRequests, 1)
			io.WriteString(w, "<html><body>leaf</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	gw := newGatedWriter()
	releaseWriter := onceCloser(gw.release)
	defer releaseWriter()
	stdin := strings.NewReader(srv.URL + "/root\n")
	done, stderr := runInBackground(t, []string{"-seed-concurrency", "1", "-t", "4", "-d", "2"}, stdin, gw)

	waitClosed(t, gw.started, "output consumer to block")

	// While the consumer is blocked, the bounded result channel fills and the
	// crawl must stop issuing new work. Requests already spawned before the
	// channel filled may still finish; the point is that the remaining crawl
	// cannot advance.
	time.Sleep(200 * time.Millisecond)
	blocked := atomic.LoadInt32(&leafRequests)
	if blocked >= links/2 {
		t.Fatalf("too many leaf requests (%d) before backpressure engaged", blocked)
	}

	releaseWriter()
	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := atomic.LoadInt32(&leafRequests); got != links {
		t.Fatalf("leaf requests after draining = %d, want %d", got, links)
	}
	if got := strings.Count(gw.String(), "/leaf/"); got != links {
		t.Fatalf("output lines = %d, want %d", got, links)
	}
}

// countingLineReader returns exactly one stdin line per Read call and counts
// how many lines the crawler actually consumed.
type countingLineReader struct {
	base  string
	total int

	mu    sync.Mutex
	reads int
}

func (r *countingLineReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reads >= r.total {
		return 0, io.EOF
	}
	line := fmt.Sprintf("%s/seed/%d\n", r.base, r.reads)
	r.reads++
	return copy(p, line), nil
}

func (r *countingLineReader) LinesRead() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

// TestStdinBackpressure proves the pending-seed queue is bounded. With one
// active (gated) seed and queue capacity one, stdin is not drained.
func TestStdinBackpressure(t *testing.T) {
	slowArrived := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	closeSlow := onceCloser(releaseSlow)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/seed/0":
			slowArrived <- struct{}{}
			select {
			case <-releaseSlow:
			case <-r.Context().Done():
			}
			io.WriteString(w, "<html><body>slow</body></html>")
		default:
			io.WriteString(w, "<html><body>fast</body></html>")
		}
	}))
	defer srv.Close()
	defer closeSlow()

	reader := &countingLineReader{base: srv.URL, total: 20}
	done, stderr := runInBackground(t, []string{"-seed-concurrency", "1", "-t", "2"}, reader, io.Discard)

	waitClosed(t, slowArrived, "slow seed request")

	// 1 active seed + 1 queued seed + 1 line already scanned but blocked
	// trying to enter the queue. The reader must not run ahead of that.
	first := reader.LinesRead()
	time.Sleep(200 * time.Millisecond)
	second := reader.LinesRead()
	if second != first {
		t.Fatalf("stdin reader kept consuming lines while the queue was full: %d -> %d", first, second)
	}
	if second > 4 {
		t.Fatalf("stdin reader consumed %d lines with a queue of 1; backpressure missing", second)
	}

	closeSlow()
	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := reader.LinesRead(); got != reader.total {
		t.Fatalf("stdin lines read after drain = %d, want %d", got, reader.total)
	}
}

// TestSeedIsolation proves each seed has independent depth, scope, and visit
// deduplication, while -u still deduplicates formatted output globally.
func TestSeedIsolation(t *testing.T) {
	var mu sync.Mutex
	requests := make(map[string]int)
	record := func(key string) {
		mu.Lock()
		requests[key]++
		mu.Unlock()
	}
	count := func(key string) int {
		mu.Lock()
		defer mu.Unlock()
		return requests[key]
	}

	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("B" + r.URL.Path)
		switch r.URL.Path {
		case "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case "/":
			io.WriteString(w, `<html><body><a href="/b1">b1</a></body></html>`)
		case "/b1":
			io.WriteString(w, "<html><body>b1</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer serverB.Close()

	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("A" + r.URL.Path)
		switch r.URL.Path {
		case "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case "/":
			fmt.Fprintf(w, `<html><body><a href="/a1">a1</a><a href="/a1">a1 again</a><a href="%s/foreign">foreign</a></body></html>`, serverB.URL)
		case "/a1":
			io.WriteString(w, `<html><body><a href="/a2">a2</a></body></html>`)
		case "/a2":
			io.WriteString(w, "<html><body>a2</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer serverA.Close()

	stdin := strings.NewReader(fmt.Sprintf("%s\n%s\n", serverA.URL, serverB.URL))
	done, stderr := runInBackground(t, []string{"-seed-concurrency", "2", "-t", "4", "-d", "2", "-i"}, stdin, io.Discard)
	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}

	// Depth is per seed: depth 3 is not fetched.
	if got := count("A/a2"); got != 0 {
		t.Fatalf("seed A fetched /a2 past depth 2 %d times", got)
	}
	if got := count("B/b1"); got != 1 {
		t.Fatalf("seed B fetched /b1 %d times, want 1", got)
	}
	// Scope is per seed: A must not follow the link into B's origin.
	if got := count("B/foreign"); got != 0 {
		t.Fatalf("seed A left its scope for B/foreign %d times", got)
	}
	// Visit deduplication is per collector: the two duplicate hrefs on A's
	// root page are fetched once, but B independently visits its own root.
	if got := count("A/a1"); got != 1 {
		t.Fatalf("seed A fetched /a1 %d times, want 1 (per-seed dedupe)", got)
	}
	if got := count("A/"); got != 1 {
		t.Fatalf("seed A root fetched %d times, want 1", got)
	}
	if got := count("B/"); got != 1 {
		t.Fatalf("seed B root fetched %d times, want 1", got)
	}

	// Same link discovered by two seeds is fetched independently by each
	// collector, and without -u both formatted results are printed.
	sharedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record("S" + r.URL.Path)
		switch r.URL.Path {
		case "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case "/one", "/two":
			io.WriteString(w, `<html><body><a href="/shared">shared</a></body></html>`)
		case "/shared":
			io.WriteString(w, "<html><body>shared</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer sharedServer.Close()

	sharedStdin := fmt.Sprintf("%s/one\n%s/two\n", sharedServer.URL, sharedServer.URL)

	for _, tc := range []struct {
		name  string
		args  []string
		count int
	}{
		{"without-u", []string{"-seed-concurrency", "2", "-t", "4", "-d", "2"}, 2},
		{"with-u", []string{"-seed-concurrency", "2", "-t", "4", "-d", "2", "-u"}, 1},
	} {
		mu.Lock()
		requests = make(map[string]int)
		mu.Unlock()

		stdout := &bytes.Buffer{}
		done, stderr := runInBackground(t, tc.args, strings.NewReader(sharedStdin), stdout)
		if code := waitDone(t, done); code != 0 {
			t.Fatalf("%s: exit code = %d, want 0 (stderr: %s)", tc.name, code, stderr.String())
		}
		if got := strings.Count(stdout.String(), "/shared"); got != tc.count {
			t.Fatalf("%s: /shared output lines = %d, want %d (output %q)", tc.name, got, tc.count, stdout.String())
		}
		if got := count("S/shared"); got != 2 {
			t.Fatalf("%s: independent collectors fetched /shared %d times, want 2", tc.name, got)
		}
	}
}

// TestNoRequestSlotLeak verifies that timeout cancellation closes in-flight
// response bodies and returns every process-wide HTTP slot. A leaked slot would
// permanently reduce the crawler's -t capacity for the rest of the run.
func TestNoRequestSlotLeak(t *testing.T) {
	hangArrived := make(chan struct{}, 1)
	releaseHang := make(chan struct{})
	closeHang := onceCloser(releaseHang)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/robots.txt":
			w.WriteHeader(http.StatusNotFound)
		case "/hang":
			hangArrived <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-releaseHang:
			}
			io.WriteString(w, "<html><body>hang</body></html>")
		case "/fast":
			io.WriteString(w, "<html><body>fast</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	defer closeHang()

	cfg := config{
		threads:         2,
		depth:           2,
		maxSize:         -1,
		timeout:         1,
		seedConcurrency: 1,
	}
	limiter := newRequestLimiter(cfg.threads)
	stdin := strings.NewReader(fmt.Sprintf("%s/hang\n%s/fast\n", srv.URL, srv.URL))
	stderr := &bytes.Buffer{}
	done := make(chan int, 1)
	go func() { done <- crawlWithLimiter(cfg, stdin, io.Discard, stderr, limiter) }()

	waitClosed(t, hangArrived, "hanging seed request")
	if code := waitDone(t, done); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr: %s)", code, stderr.String())
	}
	if got := limiter.inFlight(); got != 0 {
		t.Fatalf("%d HTTP request slots leaked after timeout", got)
	}
}

// TestFlagValidation proves non-positive -t and -seed-concurrency are rejected
// before stdin is read or any request is made.
func TestFlagValidation(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"-t", "0"}, "-t must be a positive integer"},
		{[]string{"-t", "-3"}, "-t must be a positive integer"},
		{[]string{"-seed-concurrency", "0"}, "-seed-concurrency must be a positive integer"},
		{[]string{"-seed-concurrency", "-1"}, "-seed-concurrency must be a positive integer"},
	}
	for _, tc := range cases {
		stderr := &bytes.Buffer{}
		code := run(tc.args, strings.NewReader("http://127.0.0.1:1/no-such-server\n"), io.Discard, stderr)
		if code != 1 {
			t.Fatalf("run(%v) exit code = %d, want 1", tc.args, code)
		}
		if !strings.Contains(stderr.String(), tc.want) {
			t.Fatalf("run(%v) stderr = %q, want %q", tc.args, stderr.String(), tc.want)
		}
	}
}
