# Fuze

_A mechanism to wait for a single signal which can be checked at any time._

A Fuze can be checked synchronously, and will show up as burnt only once it’s been burnt. It
can be awaited while it’s unburnt, and it can be Cloned and used without `mut` at any time.

Useful for exit conditions and as a one-off no-payload channel.

- **[API documentation](https://docs.rs/fuze)**
- Supports Async-std and Tokio.

```rust
use fuze::Fuze;
use async_std::task;
use std::time::Duration;

let f1 = Fuze::new();

let f2 = f1.clone();
task::block_on(async move {
   println!("Halo!");

  	task::spawn(async move {
  		task::sleep(Duration::from_secs(1)).await;
  		f3.burn();
  	});

  	f2.wait().await;
   println!("Adios!");
});
```

