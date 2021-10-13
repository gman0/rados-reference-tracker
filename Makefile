SRCS := main.c

all: build/reference-tracker

build/reference-tracker: $(SRCS)
	mkdir -p build
	$(CC) -o build/reference-tracker $^ -lrados -Wno-unused-parameter -Wall -Wextra -Werror -g

clean:
	rm -rf build

.PHONY: clean
