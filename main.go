package main

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/namsral/flag"
)

var (
	verbose     bool
	dryRun      bool
	url         string
	pattern     string
	limit       int
	count       int
	delay       int64
	ttlSubtract int
	ttlSet      int
	deleteKeys  bool
	ttlMin      int
	logger      struct {
		debug *log.Logger
		info  *log.Logger
	}
)

func main() {
	flag.BoolVar(&verbose, "verbose", false, "debug logging")
	flag.BoolVar(&dryRun, "dry-run", false, "dry run, no destructive commands")
	flag.StringVar(&url, "url", "redis://", "URI of Redis server (https://www.iana.org/assignments/uri-schemes/prov/redis)")
	flag.StringVar(&pattern, "pattern", "*", "Pattern of keys to process")
	flag.IntVar(&limit, "limit", 100, "Maximum number keys to process")
	flag.IntVar(&count, "count", 100, "Keys to fetch in each batch")
	flag.Int64Var(&delay, "delay", 0, "Delay in ms between batches")
	flag.IntVar(&ttlSet, "set-ttl", 0, "Set TTL in seconds of matched keys")
	flag.IntVar(&ttlSubtract, "subtract-ttl", 0, "Seconds to subtract from TTL of matched keys")
	flag.BoolVar(&deleteKeys, "delete", false, "Delete matched keys")
	flag.IntVar(&ttlMin, "ttl-min", 0, "Minimum TTL for a key to be processed. Use -1 to match no TTL.")
	flag.Parse()

	logger.debug = log.New(os.Stderr, "[debug] ", log.LstdFlags)
	logger.info = log.New(os.Stderr, "[info] ", log.LstdFlags)
	if !verbose {
		logger.debug.SetOutput(ioutil.Discard)
	}

	c, err := redis.DialURL(url)
	if err != nil {
		logger.info.Fatal("Failed to connect to redis: ", err)
	}
	defer c.Close()

	logger.debug.Println("Connected to redis server at", url)
	if dryRun {
		logger.info.Println("Dry-run mode: destructive commands skipped")
	}

	scan_stats := make(chan int, 0)
	keys_stats := make(chan int, 0)
	expired_stats := make(chan int, 0)
	go stats(scan_stats, keys_stats, expired_stats)

	var scan struct {
		cursor int
		batch  []string
		total  int
	}
	scan.cursor = 0
	scan.total = 0

	for {
		result, err := redis.Values(c.Do("SCAN", scan.cursor, "MATCH", pattern, "COUNT", count))
		if err != nil {
			logger.info.Println("Failed to execute SCAN:", err)
			time.Sleep(time.Second)
			continue
		}

		_, err = redis.Scan(result, &scan.cursor, &scan.batch)
		if err != nil {
			logger.info.Println("Failed to parse response:", err)
			time.Sleep(time.Second)
			continue
		}

		for _, key := range scan.batch {
			scan.total++

			if limit >= 0 && scan.total >= limit {
				logger.info.Println("Reached limit of", limit, "keys")
				return
			}

			if processKey(c, key) {
				expired_stats <- 1
			}
		}

		if scan.cursor == 0 {
			return
		}

		logger.debug.Println("Next cursor is", scan.cursor)
		scan_stats <- 1
		keys_stats <- len(scan.batch)

		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
	}
}

func processKey(c redis.Conn, key string) (expired bool) {
	var ttl int

	// Only fetch TTL if we need it for minimum threshold or subtracting
	if ttlMin != 0 || ttlSubtract != 0 {
		ttl, err := redis.Int(c.Do("TTL", key))
		if err != nil {
			logger.info.Println("Failed to get TTL for key", key)
			return
		}

		logger.debug.Println("TTL of", ttl, "for key", key)
	}

	if !matchTTL(ttl, ttlMin) {
		logger.debug.Println("TTL", ttl, "doesn't match minimum TTL", ttlMin)
		return
	}

	if deleteKeys {
		if dryRun {
			return true
		}

		_, err := c.Do("DEL", key)
		if err != nil {
			logger.info.Println("Failed to DELETE key", key, err)
			return
		}

		logger.debug.Println("Deleted key", key)
		return true

	}

	if ttlSubtract > 0 {
		if dryRun {
			return true
		}
		newTTL := ttl - ttlSubtract
		_, err := c.Do("EXPIRE", key, newTTL)
		if err != nil {
			logger.info.Println("Failed to EXPIRE key", key, err)
			return
		}
		logger.debug.Println("new TTL of", newTTL, "for key", key)
		return true
	}

	if ttlSet > 0 {
		if dryRun {
			return true
		}
		_, err := c.Do("EXPIRE", key, ttlSet)
		if err != nil {
			logger.info.Println("Failed to EXPIRE key", key, err)
			return
		}
		logger.debug.Println("new TTL of", ttlSet, "for key", key)
		return true
	}

	return
}

func matchTTL(ttl int, ttlMin int) bool {
	// No minimum TTL
	if ttlMin == 0 {
		return true
	}
	// Minimum TTL of a positive integer
	if ttlMin > 0 && ttl > ttlMin {
		return true
	}
	// No TTL, key won't expire
	if ttlMin == -1 && ttl == -1 {
		return true
	}
	return false
}

func stats(scans chan int, keys chan int, expired chan int) {
	timer := time.Tick(1 * time.Second)
	scan_count := 0
	keys_count := 0
	expired_count := 0

	for {
		select {
		case n := <-scans:
			scan_count += n

		case n := <-keys:
			keys_count += n

		case n := <-expired:
			expired_count += n

		case <-timer:
			logger.info.Printf("Stats: scans=%d keys=%d expires=%d\n",
				scan_count, keys_count, expired_count)
		}
	}
}
