# Root-page codec microbenchmark

`go test ./engine -run '^$' -bench BenchmarkRootPageCodec -benchmem -count=3`
on an AMD Ryzen 7 PRO 4750U (Linux, amd64). The fixture has six metrics with
seven levels each, approximating one of 256 metadata shards for 1500 metrics.
Each reference uses one of two shared pack keys.

| Codec | Encoded bytes | Encode (ns/op, 3 runs) | Decode (ns/op, 3 runs) |
| --- | ---: | ---: | ---: |
| gzip/Gob | 538 | 122971–146572 | 165588–183525 |
| versioned binary root page | 415 | 57279–61031 | 44539–51140 |

The binary page is 23% smaller on this fixture. Its encode time is roughly
halved and decode time roughly one third of Gob. This isolates the codec;
it does not measure S3 latency, metadata publication, or a running compaction.
Root-page dirtiness tracking separately avoids scanning all unchanged root
maps at every publication. Full S3 measurements require the dev setup.
