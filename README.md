[![progress-banner](https://backend.codecrafters.io/progress/redis/93ccc269-6d2d-4665-91e4-f092ba227f5a)](https://app.codecrafters.io/users/dreygur?r=2qF)

This is source for Rust solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, I built a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way I learnt about
event loops, the Redis protocol and more.

**Note**: You can head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Process

1. Ensure you have `cargo (1.94)` installed locally
2. Run `cargo build --release 2>&1` to build the project
2. Run `./spawn_redis_server.sh` to run your Redis server, which is implemented
   in `src/main.rs`
3. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.
