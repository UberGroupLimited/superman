import Gearman from 'abraxas';
import TOML from '@iarna/toml';
import glob from 'fast-glob';
import { promises as fs } from 'fs';
import Influx from 'influx';
import ms from 'ms';
import knex from 'knex';
import { config, env, kill, on } from 'process';
import Rollbar from 'rollbar';
import cp from 'child_process';
import path from 'path';
import os from 'os';
import readline from 'readline';
import { timingSafeEqual } from 'crypto';

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
	return async (task) => {
		const fn = state.functions.get(name);
		fn.running.incr();
		fn.count.incr();

		try {
			const workload = Buffer.from(task.payload);
			const workdir = await fs.mkdtemp(path.join(os.tmpdir(), name+task.uniqueid));

			await new Promise((resolve, reject) => {
				const run = cp.execFile(fn.executor, [
					name,
					task.uniqueid,
					''+workload.length,
				], {
					cwd: workdir,
					env: [],
					maxBuffer: 128 * 1024**2, // 128MB
					...state.workerOpts,
				});

				const stderr = '';
				run.stderr.on('data', (chunk) => {
					stderr += chunk.toString();
				});

				const reader = readline.createInterface({
					input: run.stdout,
					terminal: false,
				});
				reader.on('line', (line) => {
					try {
						const data = JSON.parse(line);
						switch (data.type) {
							case 'complete':
								let completion;
								if (data.error) {
									completion = data.error;
								} else {
									completion = data.data;
								}

								task.end(JSON.stringify(completion));
							break;

							case 'update':
								task.update(JSON.stringify(data.data)); // to be implemented in abraxas
							break;

							case 'progress':
								task.progress(task.numerator, task.denominator); // to be implemented in abraxas
							break;

							default:
								throw new Error(`unsupported event type: ${data.type}`);
						}
					} catch (err) {
						ohno(err);
					}
				});

				run.on('error', (err) => {
					run.removeAllListeners();
					reject(err);
				});

				run.on('exit', (code) => {
					if (code === 0) {
						run.removeAllListeners();
						resolve();
					} else {
						run.removeAllListeners();
						reject(new Error(`worker executor exited with ${code}\n${stderr}`));
					}
				});
			});
		} catch (err) {
			ohno(err);
			task.error(err);
		} finally {
			fn.running.decr();
			await fs.rmdir(workdir, {
				maxRetries: 20,
				recursive: true,
			});
		}
	};
}

async function executable(executor) {
	try {
		const fh = await fs.open(executor);
		const stat = await fh.stat();
		return !!(stat.isFile() && (stat.mode & 1));
	} catch (_) {
		return false;
	}
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

		// the executor provides a simple way to get some work done on
		// select nodes, such that if the executor (= worker program)
		// doesn't exist or isn't executable, the worker will be silently
		// skipped here. so resist the urge to make this an error e hoa!
		if (!await executable(worker.executable)) {
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
			client.concurrency(name, fn.concurrency); // to be implemented in abraxas
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

	const workerOpts = {};
	if (config.worker?.env) workerOpts.env = config.worker.env;
	if (config.worker?.timeout) workerOpts.timeout = config.worker.timeout;
	if (config.worker?.max_buffer) workerOpts.maxBuffer = config.worker.max_buffer;
	if (config.worker?.user) workerOpts.uid =
		typeof config.worker.user == 'number'
		? config.worker.user
		: userid.uid(config.worker.user);
	if (config.worker?.group) workerOpts.gid =
		typeof config.worker.group == 'number'
		? config.worker.group
		: userid.gid(config.worker.group);

	const state = {
		function: new Map,
		reloading: new AtomicBool(false),
		quitting: new AtomicBool(false),
		round: new AtomicUint(0),
		workerOpts,
	};

	await reload(config, client, state);

	// TODO: listen to USR1 and reload
	// TODO: listen to TERM and gracefully shutdown:
	//       - call client.forgetAllWorkers()
	//       - stop listening to USR1
	//       - shut down when last worker is done
})().catch(err => ohno(err, 1));
