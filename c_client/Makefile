# BenchFS Mini C Client Makefile

CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LDFLAGS = -lucp -lucs

# UCX paths (adjust if needed)
UCX_INCLUDE = /usr/local/include
UCX_LIB = /usr/local/lib

INCLUDES = -I$(UCX_INCLUDE) -I.
LIBS = -L$(UCX_LIB) $(LDFLAGS)

# Targets
TARGET_LIB = libbenchfs_mini_client.a
TARGET_TEST = test_client

# Source files
LIB_SRCS = benchfs_mini_client.c
LIB_OBJS = $(LIB_SRCS:.c=.o)

TEST_SRCS = test_client.c
TEST_OBJS = $(TEST_SRCS:.c=.o)

.PHONY: all clean test

all: $(TARGET_LIB) $(TARGET_TEST)

# Build static library
$(TARGET_LIB): $(LIB_OBJS)
	ar rcs $@ $^
	@echo "Built $@"

# Build test program
$(TARGET_TEST): $(TEST_OBJS) $(TARGET_LIB)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)
	@echo "Built $@"

# Compile object files
%.o: %.c benchfs_mini_client.h
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Clean build artifacts
clean:
	rm -f $(LIB_OBJS) $(TEST_OBJS) $(TARGET_LIB) $(TARGET_TEST)
	@echo "Cleaned build artifacts"

# Show compiler and linker commands for debugging
test:
	@echo "CC = $(CC)"
	@echo "CFLAGS = $(CFLAGS)"
	@echo "INCLUDES = $(INCLUDES)"
	@echo "LIBS = $(LIBS)"
