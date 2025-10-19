SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin

TEST_SRC_DIR = tests
TEST_OBJ_DIR = tests/obj

LIB_NAME = avstor
LIB_FILE = $(BIN_DIR)/lib$(LIB_NAME).a
LIB_OBJS = $(OBJ_DIR)/avstor.o

AVSCRDB_FILE = $(BIN_DIR)/avscrdb 
AVSCRDB_OBJS = $(TEST_OBJ_DIR)/avscrdb.o $(TEST_OBJ_DIR)/avsdb.o $(TEST_OBJ_DIR)/timer.o

CC = gcc
AR = ar

CFLAGS = -I./include -Wall -Wextra -Werror -pedantic

ifeq ($(THREAD_SAFE), 1)
	CFLAGS += -DAVSTOR_CONFIG_THREAD_SAFE=1
endif

ifeq ($(FILE_64BIT), 1)
	CFLAGS += -DAVSTOR_CONFIG_FILE_64BIT=1
endif

ifeq ($(FORCE_C11_THREADS), 1)
	CFLAGS += -DAVSTOR_CONFIG_FORCE_C11_THREADS=1
endif

ifeq ($(RELEASE), 1)
	CFLAGS += -DNDEBUG -O3 -g0
else
	CFLAGS += -D_DEBUG -g3
endif

all: $(OBJ_DIR) $(BIN_DIR) $(TEST_OBJ_DIR) $(LIB_FILE) $(AVSCRDB_FILE)

$(AVSCRDB_FILE): $(LIB_OBJS) $(AVSCRDB_OBJS)
	$(CC) $(AVSCRDB_OBJS) $(LIB_FILE) -o $@

$(OBJ_DIR):
	@mkdir -p $(OBJ_DIR)

$(TEST_OBJ_DIR):
	@mkdir -p $(TEST_OBJ_DIR)

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

$(LIB_FILE): $(LIB_OBJS)
	$(AR) rcs $@ $^

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(TEST_OBJ_DIR)/%.o: $(TEST_SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ_DIR)/*.o $(TEST_OBJ_DIR)/*.o $(LIB_FILE) $(AVSCRDB_FILE)

.PHONY: all clean
