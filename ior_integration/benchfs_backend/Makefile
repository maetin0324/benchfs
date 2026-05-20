# Build benchfs_pfind — io500 find-phase external driver for BenchFS.
#
# Links against libbenchfs.so (produced by `cargo build --release` in the
# benchfs workspace root). The shared library exports the C ABI declared
# in include/benchfs_c_api.h.

BENCHFS_ROOT  ?= $(realpath $(CURDIR)/../..)
BENCHFS_LIB   ?= $(BENCHFS_ROOT)/target/release
INCLUDE_DIR   := $(CURDIR)/include
SRC_DIR       := $(CURDIR)/src
BIN_DIR       := $(CURDIR)/bin

CC      ?= cc
CFLAGS  ?= -O2 -g -Wall -Wextra -Wno-unused-parameter
CFLAGS  += -I$(INCLUDE_DIR)
LDFLAGS += -L$(BENCHFS_LIB) -lbenchfs -Wl,-rpath,$(BENCHFS_LIB)

# Optional MPI build: set WITH_MPI=1 (and CC=mpicc) to get a binary that
# can be launched via mpirun for parallel walks. The default no-MPI build
# avoids the nested-mpirun hang when io500's rank 0 popen()s this binary.
ifdef WITH_MPI
CFLAGS  += -DBENCHFS_PFIND_WITH_MPI
endif

.PHONY: all clean pfind

all: pfind

pfind: $(BIN_DIR)/benchfs_pfind

$(BIN_DIR)/benchfs_pfind: $(SRC_DIR)/benchfs_pfind.c $(INCLUDE_DIR)/benchfs_c_api.h
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -rf $(BIN_DIR)
