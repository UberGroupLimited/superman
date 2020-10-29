import Gearman from 'abraxas';
import TOML from '@iarna/toml';
import glob from 'fast-glob';
import { promises as fs } from 'fs';
import Influx from 'influx';
import ms from 'ms';
import knex from 'knex';
import { env } from 'process';
import Rollbar from 'rollbar';

export function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms))
}

async function loadConfig() {
	const filename = (await glob([
		'./superman.toml',
		'/apps/superman/live/superman.toml',
		'/etc/superman.toml',
	]))[0];

	if (!filename) throw new Error('Cannot find a superman.toml configuration file');
	return TOML.parse(await fs.readFile(filename));
}

function handler (state, name) {
	return (task) => {
		const fn = state.functions.get(name);
		fn.running.incr();
		fn.count.incr();

		// run the thing

		fn.running.decr();
	};
}

async function reload (config, client, state) {
	if (state.quitting.load()) return;
	if (state.reloading.cas(false, true)) return;

	const gen = state.round.incr();

	const mysql = knex({
		client: 'mysql2',
		connection: config.mysql,
	});

	const workers = config.workers ||
		await mysql
		.from('pure_gearman_workers')
		.where('active', true)
		.andWhere('concurrency', '>', 0);

	for (const worker in workers) {
		if (!(worker.name || (worker.ns && worker.method))) {
			ohno(new Error(`empty name or ns+method for id/index ${worker.id || index}`));
			continue;
		}

		const name = worker.name || (worker.ns + '::' + worker.method);

		let fn = state.functions.get(name);
		if (!fn) state.functions.set(name, fn = {
			gen,
			executor: worker.executor,
			concurrency: worker.concurrency,
			running: new AtomicUint(0),
			count: new AtomicUint(0),
			handler: null,
		});

		fn.gen = gen;
		if (fn.executor != worker.executor) {
			fn.handler = null;
			client.unregisterWorker(name);
			fn.executor = worker.executor;
		}

		if (!fn.handler) {
			fn.handler = handler(state, name);
			client.registerWorker(name, fn.handler);
		}

		if (fn.concurrency != worker.concurrency) {
			client.concurrency(name, fn.concurrency);
			fn.concurrency = worker.concurrency;
		}
	}

	for (const [name, fn] of state.functions.entries()) {
		if (fn.gen != gen) {
			client.unregisterWorker(name);
			fn.handler = null;
			if (fn.running.zero()) {
				info(`Retiring ${name}; ran ${fn.count.load()} times`);
				state.functions.delete(name);
			}
		}
	}

	info(`Now running with ${state.functions.size} functions`);
	state.reloading.store(false);
}

let rollbarInstance;
function rollbar (token = null) {
	if (rollbarInstance) return rollbarInstance;
	if (!token) return;

	return rollbarInstance = new Rollbar({
		accessToken: token,
		environment: env.NODE_ENV || 'development',
		captureUncaught: true,
		captureUnhandledRejections: true,
	});
}

function info (message) {
	console.info(new Date, '|', message)
}

function ohno (err, exit = false) {
	console.error(new Date, '|', err);
	const roll = rollbar();
	if (roll) roll.error(err);
	if (exit !== false) process.exit(exit);
}

class AtomicBool {
	constructor (initial) {
		this.sab = new SharedArrayBuffer(1);
		this.ta = new Uint8Array(this.sab);
		Atomics.store(this.ta, 0, +!!initial);
	}

	load () {
		return !!Atomics.load(this.ta, 0);
	}

	store (value) {
		return !!Atomics.store(this.ta, +!!value);
	}

	cas (expected, value) {
		return !!Atomics.compareExchange(this.ta, 0, +!!expected, +!!value);
	}
}

class AtomicUint {
	constructor (initial) {
		this.sab = new SharedArrayBuffer(4);
		this.ta = new Uint32Array(this.sab);
		Atomics.store(this.ta, 0, +initial);
	}

	load () {
		return Atomics.load(this.ta, 0);
	}

	store (value) {
		return Atomics.store(this.ta, +value);
	}

	cas (expected, value) {
		return Atomics.compareExchange(this.ta, 0, +expected, +value);
	}

	incr () {
		return Atomics.add(this.ta, 0, 1);
	}

	decr () {
		return Atomics.sub(this.ta, 0, 1);
	}

	zero () {
		return this.load() === 0;
	}
}

(async () => {
	const config = await loadConfig();
	rollbar(config.rollbar);

	const client = Gearman.Client.connect({
		servers: [config.gearman],
	});

	const state = {
		function: new Map,
		reloading: new AtomicBool(false),
		quitting: new AtomicBool(false),
		round: new AtomicUint(0),
	};

	await reload(config, client, state);

	// TODO: listen to USR1 and reload
	// TODO: listen to TERM and gracefully shutdown:
	//       - call client.forgetAllWorkers()
	//       - stop listening to USR1
	//       - shut down when last worker is done
})().catch(err => ohno(err, 1));
