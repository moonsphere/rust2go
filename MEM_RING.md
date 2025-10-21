# Mem Ring

A ring based on shared memory bridging Rust and Go. The implementation now targets the Tokio runtime.

With 2 rings, users can simulate calls between rust and go(Both sides can start calls).

## How it Works
TODO

## Runtime Support

Mem Ring requires Tokio for its asynchronous helpers. Add it to your project with:

```toml
[dependencies]
mem-ring = "0.2"
```
