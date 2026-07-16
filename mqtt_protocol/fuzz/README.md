# Fuzzing rust-mqtt-protocol

Coverage-guided fuzzing with [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer).

## Targets

- **`decode`** — feeds arbitrary bytes through `FixedHeader::parse`, dispatches the body to
  every packet type for both MQTT v3.1.1 and v5.0, and round-trips every successful decode
  (`decode → encode → decode`), asserting the results are equal. Catches panics in the decoders
  and encode/decode asymmetries.
- **`fixed_header`** — fuzzes only `FixedHeader::parse`. The first 4 bytes of the input are the
  little-endian `max_packet_size` (mod `MAX_MQTT_PACKET_SIZE + 1`), the rest is the packet, so
  the `PacketTooLarge` boundary is exercised too.

## Setup

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Running

From `mqtt_protocol/`:

```sh
cargo +nightly fuzz run decode -- -dict=mqtt.dict
cargo +nightly fuzz run fixed_header
```

Useful flags: `-max_total_time=300` (bounded run), `-jobs=8` (parallel workers).

## Coverage reports

Coverage tells you which parsers the fuzzer actually reaches — a parser stuck at low
coverage is one whose bugs the fuzzer would *not* find.

One-time prerequisite (installs `llvm-cov` / `llvm-profdata`):

```sh
rustup component add llvm-tools-preview --toolchain nightly
```

Then use the `Makefile` in this directory:

```sh
make coverage                     # collect + text summary + HTML for `decode`
make coverage TARGET=fixed_header # a different target
make report                       # text summary only, from existing data
make html                         # HTML report only; then open coverage/<target>/html/index.html
make clean                        # remove generated coverage data/reports
```

`make coverage` runs `cargo +nightly fuzz coverage <target>` over the corpus, then renders the
merged profile against the instrumented binary with `llvm-cov`. Output lands under
`coverage/<target>/` (gitignored).

Reading it:

- The **summary table** shows per-file line/region/branch %. Every packet parser
  (`connect`, `connack`, `publish`, …, `unsuback`) should be high — that is the evidence that
  malformed-input bugs in them would be caught immediately. A file near 0% is a reachability
  gap; steer the fuzzer there with a seed or a `mqtt.dict` entry.
- The **HTML report** marks never-executed lines with a `0` count — use it to spot a packet
  type or property arm the fuzzer never explores.
- This is **decode-side** coverage. The encoders will look under-covered by construction:
  raw-byte fuzzing rarely builds deep valid packets to re-encode. Closing that needs a
  structured (`arbitrary`-based) target, not more seeds.

## Reproducing a crash

Crashing inputs are written to `artifacts/<target>/`. Re-run one with:

```sh
cargo +nightly fuzz run decode artifacts/decode/crash-<hash>
```

Minimize it with:

```sh
cargo +nightly fuzz tmin decode artifacts/decode/crash-<hash>
```

A crash is a finding about the protocol crate (panic or failed round-trip invariant), not about
the fuzzer.

## Seeds and corpus

Two directories, only one of which is committed:

- **`seeds/<target>/`** (committed) — a small curated set of valid packets, one of each type/version,
  encoded by the crate itself. This is the shared starting point that ships with the repo.
- **`corpus/<target>/`** (gitignored) — the working corpus libFuzzer grows as it runs. It accumulates
  every coverage-increasing input and can reach tens of thousands of files, so it is kept out of git.

Seed a fresh run by pointing libFuzzer at both directories (it writes new finds into the first one):

```sh
cargo +nightly fuzz run decode corpus/decode seeds/decode -- -dict=mqtt.dict
```

Shrink the working corpus to a minimal coverage-equivalent set with:

```sh
cargo +nightly fuzz cmin decode
```

To persist the full accumulated corpus across machines/CI, store it externally (a corpus bucket or
CI cache artifact) rather than committing it.
