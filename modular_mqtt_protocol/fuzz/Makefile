# Coverage reports for the fuzz targets.
#
#   make coverage                     # collect + report + HTML for the decode target
#   make coverage TARGET=fixed_header # same, for another target
#   make report   TARGET=decode       # text summary from existing profdata
#   make html     TARGET=decode       # HTML report from existing profdata
#   make open     TARGET=decode       # open the HTML report in a browser
#   make clean                        # remove generated coverage data/reports
#
# Requires the nightly llvm-tools component (ships llvm-cov / llvm-profdata):
#   rustup component add llvm-tools-preview --toolchain nightly

TARGET ?= decode

# llvm-cov shipped inside the nightly toolchain (resolved, not hard-coded).
HOST_TRIPLE := $(shell rustc +nightly -vV | sed -n 's/^host: //p')
LLVM_COV    := $(shell rustc +nightly --print sysroot)/lib/rustlib/$(HOST_TRIPLE)/bin/llvm-cov

PROFDATA := coverage/$(TARGET)/coverage.profdata
HTML_DIR := coverage/$(TARGET)/html
# Instrumented binary cargo-fuzz builds under fuzz/target for the coverage profile.
# Recursive (=) so it is resolved after the build step has produced it.
COV_BIN   = $(shell find target -path '*coverage*release/$(TARGET)' -type f | head -1)
# Keep the report to this crate: drop std, dependencies and the fuzz harness itself.
IGNORE   := /rustc/|\.cargo/|/fuzz/

.PHONY: coverage report html open clean

# Full pipeline: collect fresh coverage data, then print a summary and build the HTML report.
# `cargo fuzz` is run from the parent crate dir, where it expects to find ./fuzz.
coverage:
	cd .. && cargo +nightly fuzz coverage $(TARGET)
	$(MAKE) report TARGET=$(TARGET)
	$(MAKE) html   TARGET=$(TARGET)

# Per-file line/region/branch coverage table.
report: $(PROFDATA)
	@test -n "$(COV_BIN)" || { echo "No coverage binary for '$(TARGET)'. Run: make coverage TARGET=$(TARGET)"; exit 1; }
	$(LLVM_COV) report "$(COV_BIN)" -instr-profile=$(PROFDATA) \
		-ignore-filename-regex='$(IGNORE)' -use-color

# Line-by-line HTML report; cold lines show a 0 execution count.
html: $(PROFDATA)
	@test -n "$(COV_BIN)" || { echo "No coverage binary for '$(TARGET)'. Run: make coverage TARGET=$(TARGET)"; exit 1; }
	$(LLVM_COV) show "$(COV_BIN)" -instr-profile=$(PROFDATA) \
		-format=html -output-dir=$(HTML_DIR) \
		-show-line-counts-or-regions \
		-ignore-filename-regex='$(IGNORE)'
	@echo "HTML report: $(HTML_DIR)/index.html"

open: html
	xdg-open $(HTML_DIR)/index.html >/dev/null 2>&1 || echo "Open $(HTML_DIR)/index.html manually"

# Fail helpfully if coverage data has not been collected yet.
$(PROFDATA):
	@echo "No merged profile at $(PROFDATA). Run: make coverage TARGET=$(TARGET)"
	@exit 1

clean:
	rm -rf coverage/*/html coverage/*/coverage.profdata coverage/*/raw
