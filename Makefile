# Compiler and Flags
CC = clang
CFLAGS = -Wall -Wextra -Werror -pedantic -pthread
LDFLAGS = -pthread

# Source Files
SRCS = httpserver.c
OBJS = $(SRCS:.c=.o)

# Library Files
LIBS = asgn4_helper_funcs.a

# Include Headers (if needed)
INCLUDES = -I.

# Executable Name
TARGET = httpserver

# Build Executable
all: $(TARGET)

$(TARGET): $(OBJS) $(LIBS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $(TARGET) $(OBJS) $(LIBS)

# Compile Object Files
%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Clean Build Files
clean:
	rm -f $(OBJS) $(TARGET)
