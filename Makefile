# -fsanitize=undefined
CXXFLAGS += -std=c++11 -O3 -g -Wall -Wextra -I/usr/include/rados -I/usr/include/jsoncpp -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
# -Wa,-adhln -g
# -lprofiler
LDFLAGS += -pthread -lrados -ljsoncpp -lstdc++ -lm -g -ltcmalloc

#CC=clang-6.0

main: main.o mysignals.o radosutil.o
	$(CC) $^ -o $@ $(LDFLAGS)

clean:
	rm -f main.o mysignals.o radosutil.o ./main

.cpp.o:
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

indent: *.cpp *.h
	clang-format-6.0 -i $^

builddep:
	sudo apt install -y --no-install-recommends libjsoncpp-dev libtcmalloc-minimal4
