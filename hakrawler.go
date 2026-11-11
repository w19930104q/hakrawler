package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
)

type Result struct {
	Source string
	URL    string
	Where  string
}

const defaultUserAgent = "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"

const noURLsMessage = "No URLs were found. This usually happens when a domain is specified (https://example.com), but it redirects to a subdomain (https://www.example.com). The subdomain is not included in the scope, so the no URLs are printed. In order to overcome this, either specify the final URL in the redirect chain or use the -subs option to include subdomains."

// config holds all values that affect crawling. It is built once, before any
// request is made, so invalid flag combinations can be rejected early.
type config struct {
	inside           bool
	threads          int
	depth            int
	maxSize          int
	insecure         bool
	subsInScope      bool
	showJson         bool
	showSource       bool
	showWhere        bool
	headers          map[string]string
	unique           bool
	proxyURL         *url.URL
	timeout          int
	disableRedirects bool
	seedConcurrency  int
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run parses flags, validates them, and crawls. It returns the process exit
// code. Keeping it separate from main makes the crawler testable with in-memory
// stdin/stdout.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("hakrawler", flag.ContinueOnError)
	fs.SetOutput(stderr)

	inside := fs.Bool("i", false, "Only crawl inside path")
	threads := fs.Int("t", 8, "Number of threads to utilise.")
	depth := fs.Int("d", 2, "Depth to crawl.")
	maxSize := fs.Int("size", -1, "Page size limit, in KB.")
	insecure := fs.Bool("insecure", false, "Disable TLS verification.")
	subsInScope := fs.Bool("subs", false, "Include subdomains for crawling.")
	showJson := fs.Bool("json", false, "Output as JSON.")
	showSource := fs.Bool("s", false, "Show the source of URL based on where it was found. E.g. href, form, script, etc.")
	showWhere := fs.Bool("w", false, "Show at which link the URL is found.")
	rawHeaders := fs.String("h", "", "Custom headers separated by two semi-colons. E.g. -h \"Cookie: foo=bar;;Referer: http://example.com/\" ")
	unique := fs.Bool("u", false, "Show only unique urls.")
	proxy := fs.String("proxy", "", "Proxy URL. E.g. -proxy http://127.0.0.1:8080")
	timeout := fs.Int("timeout", -1, "Maximum time to crawl each URL from stdin, in seconds.")
	disableRedirects := fs.Bool("dr", false, "Disable following HTTP redirects.")
	seedConcurrency := fs.Int("seed-concurrency", 1, "Maximum number of stdin URLs crawled concurrently.")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	// Validate before reading stdin or making any HTTP request.
	if *threads <= 0 {
		fmt.Fprintln(stderr, "Error: -t must be a positive integer")
		return 1
	}
	if *seedConcurrency <= 0 {
		fmt.Fprintln(stderr, "Error: -seed-concurrency must be a positive integer")
		return 1
	}

	// Check for stdin input when running as a CLI. Tests and embedders pass a
	// plain reader, which is always considered piped input.
	if f, ok := stdin.(*os.File); ok {
		stat, _ := f.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			fmt.Fprintln(stderr, "No urls detected. Hint: cat urls.txt | hakrawler")
			return 1
		}
	}

	var proxyURL *url.URL
	if *proxy != "" {
		os.Setenv("PROXY", *proxy)
		proxyURL, _ = url.Parse(os.Getenv("PROXY"))
	}

	headers, err := parseHeaders(*rawHeaders)
	if err != nil {
		fmt.Fprintln(stderr, "Error parsing headers:", err)
		return 1
	}

	cfg := config{
		inside:           *inside,
		threads:          *threads,
		depth:            *depth,
		maxSize:          *maxSize,
		insecure:         *insecure,
		subsInScope:      *subsInScope,
		showJson:         *showJson,
		showSource:       *showSource,
		showWhere:        *showWhere,
		headers:          headers,
		unique:           *unique,
		proxyURL:         proxyURL,
		timeout:          *timeout,
		disableRedirects: *disableRedirects,
		seedConcurrency:  *seedConcurrency,
	}

	return crawl(cfg, stdin, stdout, stderr)
}

// crawl runs the seed scheduler and the output consumer. Seeds are read in
// stdin order into a bounded queue. A fixed pool of seedConcurrency workers
// activates them, so at most seedConcurrency seeds crawl at once while the
// queue stays bounded and applies backpressure to stdin.
func crawl(cfg config, stdin io.Reader, stdout, stderr io.Writer) int {
	return crawlWithLimiter(cfg, stdin, stdout, stderr, newRequestLimiter(cfg.threads))
}

func crawlWithLimiter(cfg config, stdin io.Reader, stdout, stderr io.Writer, limiter *requestLimiter) int {
	logger := log.New(stderr, "", log.LstdFlags)

	baseTransport := &http.Transport{
		Proxy:           nil,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.insecure},
	}
	if cfg.proxyURL != nil && cfg.proxyURL.String() != "" {
		baseTransport.Proxy = http.ProxyURL(cfg.proxyURL)
	}
	defer baseTransport.CloseIdleConnections()

	results := make(chan string, cfg.threads)

	// The queue has room for seedConcurrency pending seeds. Once both the
	// active workers and this queue are full, the stdin reader blocks.
	seeds := make(chan string, cfg.seedConcurrency)

	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		defer close(seeds)

		s := bufio.NewScanner(stdin)
		for s.Scan() {
			seedURL := s.Text()
			if _, err := extractHostname(seedURL); err != nil {
				logger.Println("Error parsing URL:", err)
				continue
			}
			seeds <- seedURL
		}
		if err := s.Err(); err != nil {
			fmt.Fprintln(stderr, "reading standard input:", err)
		}
	}()

	var workersWG sync.WaitGroup
	workersWG.Add(cfg.seedConcurrency)
	for i := 0; i < cfg.seedConcurrency; i++ {
		go func() {
			defer workersWG.Done()
			for seedURL := range seeds {
				crawlSeed(seedURL, cfg, limiter, baseTransport, results, logger)
			}
		}()
	}

	// Do not close the output before every seed (and every request spawned by
	// a seed) has exited. The reader must be done before the workers can
	// finish, so wait for both.
	go func() {
		readerWG.Wait()
		workersWG.Wait()
		close(results)
	}()

	w := bufio.NewWriter(stdout)
	defer w.Flush()

	urlsFound := false
	seen := make(map[string]struct{})
	for res := range results {
		if cfg.unique {
			if _, ok := seen[res]; ok {
				continue
			}
			seen[res] = struct{}{}
		}
		fmt.Fprintln(w, res)
		urlsFound = true
	}

	if !urlsFound {
		fmt.Fprintln(stderr, noURLsMessage)
	}
	return 0
}

// crawlSeed crawls one stdin URL with its own collector. Collectors keep
// independent depth counters, domain scope, and visited-URL storage. The
// timeout starts when this seed is activated (when a worker picks it up).
func crawlSeed(seedURL string, cfg config, limiter *requestLimiter, base http.RoundTripper, results chan<- string, logger *log.Logger) {
	var ctx context.Context
	var cancel context.CancelFunc
	if cfg.timeout == -1 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(cfg.timeout)*time.Second)
	}
	defer cancel()

	hostname, err := extractHostname(seedURL)
	if err != nil {
		logger.Println("Error parsing URL:", err)
		return
	}

	allowedDomains := []string{hostname}
	// if "Host" header is set, append it to allowed domains
	if cfg.headers != nil {
		if val, ok := cfg.headers["Host"]; ok {
			allowedDomains = append(allowedDomains, val)
		}
	}

	// Instantiate a collector per seed. Do not share its visited store or
	// depth counter with other seeds.
	c := colly.NewCollector(
		// default user agent header
		colly.UserAgent(defaultUserAgent),
		// set custom headers
		colly.Headers(cfg.headers),
		// limit crawling to the domain of the specified URL
		colly.AllowedDomains(allowedDomains...),
		// set MaxDepth to the specified depth
		colly.MaxDepth(cfg.depth),
		// asynchronous requests, bounded process-wide by the shared limiter
		colly.Async(true),
		// cancel every request (including redirect hops and robots.txt) when
		// this seed times out
		colly.StdlibContext(ctx),
	)

	// set a page size limit
	if cfg.maxSize != -1 {
		c.MaxBodySize = cfg.maxSize * 1024
	}

	// if -subs is present, use regex to filter out subdomains in scope.
	if cfg.subsInScope {
		c.AllowedDomains = nil
		c.URLFilters = []*regexp.Regexp{regexp.MustCompile(".*(\\.|\\/\\/)" + strings.ReplaceAll(hostname, ".", "\\.") + "((#|\\/|\\?).*)?")}
	}

	// If `-dr` flag provided, do not follow HTTP redirects.
	if cfg.disableRedirects {
		c.SetRedirectHandler(func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		})
	}

	// add the custom headers
	if cfg.headers != nil {
		c.OnRequest(func(r *colly.Request) {
			for header, value := range cfg.headers {
				r.Headers.Set(header, value)
			}
		})
	}

	// All seeds share one transport so the limiter caps in-flight HTTP for
	// the whole process, not just per collector.
	c.WithTransport(&seedTransport{
		base:    base,
		ctx:     ctx,
		limiter: limiter,
	})

	emitter := newResultEmitter(ctx, results)

	seedStillActive := func() bool {
		return ctx.Err() == nil
	}

	// Print every href found, and visit it
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		if !seedStillActive() {
			return
		}
		link := e.Attr("href")
		absLink := e.Request.AbsoluteURL(link)
		if strings.Contains(absLink, seedURL) || !cfg.inside {
			emitter.send(formatResult(link, "href", cfg.showSource, cfg.showWhere, cfg.showJson, e))
			// Never append more work after a timeout/cancellation.
			if seedStillActive() {
				e.Request.Visit(link)
			}
		}
	})

	// find and print all the JavaScript files
	c.OnHTML("script[src]", func(e *colly.HTMLElement) {
		if !seedStillActive() {
			return
		}
		emitter.send(formatResult(e.Attr("src"), "script", cfg.showSource, cfg.showWhere, cfg.showJson, e))
	})

	// find and print all the form action URLs
	c.OnHTML("form[action]", func(e *colly.HTMLElement) {
		if !seedStillActive() {
			return
		}
		emitter.send(formatResult(e.Attr("action"), "form", cfg.showSource, cfg.showWhere, cfg.showJson, e))
	})

	// Start scraping. Visit only queues the initial request when Async is on;
	// Wait covers every request spawned by it, including queued link visits.
	c.Visit(seedURL)
	c.Wait()

	if ctx.Err() == context.DeadlineExceeded {
		logger.Println("[timeout] " + seedURL)
	}
}

// requestLimiter is a process-wide counting semaphore for incomplete HTTP
// requests. A token is held from the start of RoundTrip until the response
// body is closed, so redirect hops and response bodies both count.
type requestLimiter struct {
	slots chan struct{}
}

func newRequestLimiter(n int) *requestLimiter {
	return &requestLimiter{slots: make(chan struct{}, n)}
}

func (l *requestLimiter) acquire(ctx context.Context) error {
	select {
	case l.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *requestLimiter) release() {
	<-l.slots
}

// inFlight reports how many tokens are currently held. It is used by tests to
// verify that timeouts and aborted requests release their slots.
func (l *requestLimiter) inFlight() int {
	return len(l.slots)
}

// seedTransport applies one seed's cancellation context to every request made
// by its collector and admits requests through the shared process-wide
// limiter. Substituting the seed context matters for colly's robots.txt fetch,
// which otherwise uses a background context and could survive a timeout.
type seedTransport struct {
	base    http.RoundTripper
	ctx     context.Context
	limiter *requestLimiter
}

func (t *seedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if ctx == nil || ctx.Done() == nil {
		ctx = t.ctx
		req = req.Clone(ctx)
	}

	if err := t.limiter.acquire(ctx); err != nil {
		return nil, err
	}

	resp, err := t.base.RoundTrip(req)
	if err != nil {
		t.limiter.release()
		return nil, err
	}
	if resp.Body == nil {
		t.limiter.release()
		return resp, nil
	}
	resp.Body = &releaseOnClose{ReadCloser: resp.Body, release: t.limiter.release}
	return resp, nil
}

// releaseOnClose returns the limiter token exactly once, when the response
// body is closed. http.Client closes intermediate redirect bodies before
// following the next hop, so redirects consume one slot at a time rather than
// holding a slot for the entire chain.
type releaseOnClose struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (r *releaseOnClose) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.release)
	return err
}

// resultEmitter writes results for one seed. Once the seed's context is
// cancelled, pending and future results are dropped, so a timeout cannot
// produce late output.
type resultEmitter struct {
	ctx     context.Context
	results chan<- string
}

func newResultEmitter(ctx context.Context, results chan<- string) *resultEmitter {
	return &resultEmitter{ctx: ctx, results: results}
}

func (e *resultEmitter) send(result string) {
	if result == "" || e.ctx.Err() != nil {
		return
	}
	// The send is cancelable as well: a slow output consumer must not keep an
	// expired seed's callback (and therefore its collector Wait) alive.
	select {
	case e.results <- result:
	case <-e.ctx.Done():
	}
}

// parseHeaders does validation of headers input and returns a formatted map.
func parseHeaders(rawHeaders string) (map[string]string, error) {
	if rawHeaders == "" {
		return nil, nil
	}
	if !strings.Contains(rawHeaders, ":") {
		return nil, errors.New("headers flag not formatted properly (no colon to separate header and value)")
	}

	headers := make(map[string]string)
	for _, header := range strings.Split(rawHeaders, ";;") {
		var parts []string
		if strings.Contains(header, ": ") {
			parts = strings.SplitN(header, ": ", 2)
		} else if strings.Contains(header, ":") {
			parts = strings.SplitN(header, ":", 2)
		} else {
			continue
		}
		headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return headers, nil
}

// extractHostname() extracts the hostname from a URL and returns it
func extractHostname(urlString string) (string, error) {
	u, err := url.Parse(urlString)
	if err != nil || !u.IsAbs() {
		return "", errors.New("Input must be a valid absolute URL")
	}

	return u.Hostname(), nil
}

// formatResult constructs an output line, preserving the existing formats.
func formatResult(link string, sourceName string, showSource bool, showWhere bool, showJson bool, e *colly.HTMLElement) string {
	result := e.Request.AbsoluteURL(link)
	if result == "" {
		return ""
	}

	whereURL := e.Request.URL.String()
	if showJson {
		where := ""
		if showWhere {
			where = whereURL
		}
		bytes, _ := json.Marshal(Result{
			Source: sourceName,
			URL:    result,
			Where:  where,
		})
		return string(bytes)
	}
	if showSource {
		result = "[" + sourceName + "] " + result
	}
	if showWhere {
		result = "[" + whereURL + "] " + result
	}
	return result
}
