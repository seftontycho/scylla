# scylla
A naive approach to running arbitrary rust code on a remote system.

# how it works
turn rust src into a tar.gz file -> send via tcp -> decompress -> run with cargo and communicate via stdin/out -> send results back over tcp

# how to test
run `cargo run --bin node` and then run `cargo run --bin root` separately

# what is done so far
compression, sending, decompressing, and running tasks, requesting archives, and sending results back, logging, naive 1-to-many communication

# what needs to be done (in rough order)
A better TaskResult struct that can handle more than just strings, and better 1-many communication.

