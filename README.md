# One Billion Row Challenge

Simple implementation using parallel processing of the input file split into chunks.

## Results

| Machine                                                                    | Result (m:s.ms) |
| -------------------------------------------------------------------------- | --------------- |
| Intel Core i5-11600K, 32GB DDR4 3200MHz CL16, Samsung 980 PRO NVMe M.2 SSD | 00:17.338       |
| MacBook Pro (13-inch, M1, 2020) / MacBookPro17,1                           | 00:28.612       |


## TODO

Benchmarking with Go's profiling tools currently shows a significant amount of time spent in hash table lookups and a reasonable amount of time with builtin float parsing, so reduced hash table lookups and a faster float conversion (perhaps we can get away with less accuracy given the requirements).
