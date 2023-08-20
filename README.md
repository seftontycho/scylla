# scylla
A naive approach to running arbitrary code on a remote system.

# how it works
turn rust src into a tar.gz file -> send via tcp -> decompress -> run with cargo and communicate via stdin/out -> send results back over tcp

# what is done so far
compression, sending, and decompressing almost work
