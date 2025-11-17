# Makefile for BenchFS C Client

CC = gcc
CFLAGS = -Wall -Wextra -O2 -g -fPIC
LDFLAGS = -lucp -lucs

# Source files
CLIENT_SRCS = benchfs_client.c benchfs_ops.c
CLIENT_OBJS = $(CLIENT_SRCS:.c=.o)
CLIENT_LIB = libbenchfs_client.a

# C API implementation (for IOR integration)
C_API_SRCS = benchfs_c_api.c benchfs_client.c benchfs_ops.c
C_API_OBJS = $(C_API_SRCS:.c=.o)
C_API_LIB = libbenchfs_c_api.so

# Test program
TEST_SRC = test_client.c
TEST_BIN = test_client

# Default target
all: $(CLIENT_LIB) $(C_API_LIB) $(TEST_BIN)

# Build static library
$(CLIENT_LIB): $(CLIENT_OBJS)
	ar rcs $@ $^
	@echo "Built static library: $@"

# Build shared library for IOR integration (implements benchfs_c_api.h)
$(C_API_LIB): $(C_API_OBJS)
	$(CC) -shared -o $@ $^ $(LDFLAGS)
	@echo "Built shared library: $@"

# Build test program
$(TEST_BIN): $(TEST_SRC) $(CLIENT_LIB)
	$(CC) $(CFLAGS) -o $@ $(TEST_SRC) -L. -lbenchfs_client $(LDFLAGS)
	@echo "Built test program: $@"

# Compile source files
%.o: %.c benchfs_client.h benchfs_protocol.h
	$(CC) $(CFLAGS) -c $< -o $@

# Clean
clean:
	rm -f $(CLIENT_OBJS) $(C_API_OBJS) $(CLIENT_LIB) $(C_API_LIB) $(TEST_BIN)
	@echo "Cleaned build artifacts"

# Install both libraries
install: $(CLIENT_LIB) $(C_API_LIB)
	install -d $(DESTDIR)/usr/local/lib
	install -m 644 $(CLIENT_LIB) $(DESTDIR)/usr/local/lib/
	install -m 755 $(C_API_LIB) $(DESTDIR)/usr/local/lib/
	install -d $(DESTDIR)/usr/local/include/benchfs
	install -m 644 benchfs_client.h $(DESTDIR)/usr/local/include/benchfs/
	install -m 644 benchfs_protocol.h $(DESTDIR)/usr/local/include/benchfs/
	ldconfig
	@echo "Installed BenchFS client libraries"

.PHONY: all clean install
