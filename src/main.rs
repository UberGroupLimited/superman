use std::time::Duration;

use color_eyre::eyre::Result;
use state::State;
use structopt::StructOpt;

mod protocols;
mod state;

#[derive(StructOpt, Debug)]
struct Args {
	#[structopt(short = "V", long, default_value = "info", env = "SUPERMAN_VERBOSE")]
	pub log_level: log::Level,

	#[structopt(
		short,
		long,
		default_value = "127.0.0.1:4730",
		env = "SUPERMAN_CONNECT"
	)]
	pub connect: String,
}

#[async_std::main]
async fn main() -> Result<()> {
	color_eyre::install()?;
	let args = Args::from_args_safe()?;

	stderrlog::new()
		.verbosity(match args.log_level {
			log::Level::Error => 0,
			log::Level::Warn => 1,
			log::Level::Info => 2,
			log::Level::Debug => 3,
			log::Level::Trace => 4,
		})
		.timestamp(stderrlog::Timestamp::Millisecond)
		.show_module_names(true)
		.module("superman")
		.init()?;

	let state = State::create(args.connect).await?;
	state.start_worker(
		"Test::sleep",
		"/home/code/php/ucontrol/bin/run-order",
		2,
		Duration::from_secs(120),
	);

	async_std::task::sleep(std::time::Duration::from_secs(15)).await;

	state.stop_worker("Test::sleep").await?;

	state.wait().await;

	Ok(())
}
