# pelo
A quick and dirty task prioritization library and tool based on democratic voting and Elo ranking.

## How to use the library

You might not even know what this project is about.
More documentation to come. This is all very quick-and-dirty.

For now... basically...

- Create an object that implements `pelo::Persistence` (for example an instance of `pelo::SQLitePersistence`, or you can implement your own type that uses a different database or backend and still implements the `pelo::Persistence` trait).
- Create a `pelo::Engine` struct.
- You can then use the `pelo::Engine` functions to get a question (i.e. two random open tasks), answer a question (i.e. submit a vote that a task is more important than another), and get the current Elo ranking of the tasks.

