# Binary held deltas and state pages

Measured on 2026-10-05 with `TestReviewMetadataHourlyWorkload` (`review`
build tag), case `dense`: 1500 metrics at 1 sample per second, two virtual
hours (10.8 million samples), holding enabled, `interval_min` 1 s, memory
object store, real WAL fsync, compression and recovery. Each version ran
twice, alternating, under a 6 GB memory limit; the development stack ran in
parallel, so absolute CPU times vary by about 15 % between runs (data block
encoding, unchanged code, serves as the noise reference).

Reference: `c2954df` (held deltas and state pages as gob plus gzip).
Change: binary envelope kinds 4 (held delta) and 5 (state page), plus
`planHold` sorting streams only under memory pressure, filtering per-stream
delta lists only when a delta became obsolete, and `splitStreamKey` without
`fmt.Sscan`.

## CPU seconds per run (cumulative, mean of two runs)

| Part | Before | After | Change |
| --- | ---: | ---: | ---: |
| checkpoint flush goroutine (held delta encoding, index updates) | 41.5 | 23.9 | −42 % |
| `planHold` (under the engine mutex) | 15.8 | 9.7 | −39 % |
| state page encoding (`encodeManifest`) | 11.9 | 8.4 | −29 % |
| loading held deltas on recovery (`loadHeld`) | 7.7 | 4.5 | −42 % |
| data block encoding (unchanged, noise reference) | 41.6 | 37.2 | – |

Single runs: before 41.3/41.7, 15.7/15.9, 12.0/11.9, 7.4/8.0, 40.1/43.2;
after 23.2/24.6, 9.4/10.0, 8.1/8.6, 4.3/4.8, 35.1/39.3.

## Object store writes in two hours

| Kind | Before | After |
| --- | ---: | ---: |
| held | 363 MB | 263 MB |
| state | 90 MB | 66.5 MB |
| data | 426 MB | 426 MB |

Recovery from the object store (loading state, roots and held deltas): 8.9 s
before, 5.5 s after.

## Microbenchmarks

- Held delta, 6000 records of 30 streams: 21 476 bytes and 5.8 ms (gob)
  versus 6 087 bytes and 1.4 ms (binary); the synthetic values compress
  better than real ones.
- State page with 6 series: 807 bytes (gob) versus 440 bytes (binary).

The remaining largest part is gzip (BestSpeed) of data blocks.
