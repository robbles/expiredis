**Warning:** *this is alpha software. If you're going to use it in production, please read the source code first.*

```
Usage of ./expiredis:
  -count=100: Keys to fetch in each batch
  -delay=0: Delay in ms between batches
  -delete=false: Delete matched keys
  -dry-run=false: dry run, no destructive commands
  -limit=100: Maximum number keys to process
  -pattern="*": Pattern of keys to process
  -set-ttl=0: Set TTL in seconds of matched keys
  -subtract-ttl=0: Seconds to subtract from TTL of matched keys
  -ttl-min=0: Minimum TTL for a key to be processed. Use -1 to match no TTL.
  -url="redis://": URI of Redis server (https://www.iana.org/assignments/uri-schemes/prov/redis)
  -verbose=false: debug logging
```

## Examples

Set a TTL of 3600 (1 hour) for all keys without a TTL, processing 1000 keys at a time

```bash
expiredis -limit -1 -count 1000 -set-ttl 3600
```

Delete all keys matching `foo:*`
```bash
expiredis -limit -1 -pattern "foo:*" -delete
```

Reduce expiry by 86400 seconds of keys matching `bar:*` with a minimum TTL of 86400 (1 day)
```bash
expiredis -limit -1 -pattern "bar:*" -subtract-ttl 86400
```
