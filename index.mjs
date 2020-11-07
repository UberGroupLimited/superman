import Gearman from 'abraxas';
import Influx from 'influx';
import Rollbar from 'rollbar';
import TOML from '@iarna/toml';
import bytes from 'bytes';
import cp from 'child_process';
import glob from 'fast-glob';
import knex from 'knex';
import ms from 'ms';
import os from 'os';
import path from 'path';
import readline from 'readline';
import { config, env, kill } from 'process';
import { promises as fs } from 'fs';

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
	return (task) => (async () => {
		const fn = state.functions.get(name);

		const r = fn.running.incr();
		const n = fn.count.incr();
		info(`Running ${name} [${r + 1}|${n + 1}]`);

		const workload = Buffer.from(task.payload);
		const workdir = await fs.mkdtemp(path.join(os.tmpdir(), name+task.uniqueid));

		try {
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

				let stderr = '';
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
								delete data.type;
								task.end(JSON.stringify(data));
								run.removeAllListeners();
								resolve();
							break;

							case 'update':
								task.update(JSON.stringify(data.data));
							break;

							case 'progress':
								task.progress(task.numerator, task.denominator);
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

				run.stdin.end(workload);
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
	})().catch(ohno);
}

async function executable(executor) {
	try {
		const fh = await fs.open(executor);
		const stat = await fh.stat();
		fh.close();
		return !!(stat.isFile() && (stat.mode & 1));
	} catch (_) {
		return false;
	}
}

async function reload (config, state) {
	if (state.reloading.cas(false, true)) return;

	const gen = state.round.incr();

	if (!state.quitting.load()) {
		const mysql = knex({
			client: 'mysql2',
			connection: config.mysql,
		});

		const functions = config.functions ||
			await mysql
			.from('pure_gearman_functions')
			.where('active', true)
			.andWhere('concurrency', '>', 0);

		for (const [index, def] of functions.entries()) {
			if (!(def.name || (def.ns && def.method))) {
				ohno(new Error(`empty name or ns+method for id/index ${def.id || index}`));
				continue;
			}

			// the executor provides a simple way to get some work done on
			// select nodes, such that if the executor (= worker program)
			// doesn't exist or isn't executable, the definition will be silently
			// skipped here. so resist the urge to make this an error e hoa!
			if (!await executable(def.executor)) {
				continue;
			}

			const name = def.name || (def.ns + '::' + def.method);

			let fn = state.functions.get(name);
			if (!fn) state.functions.set(name, fn = {
				gen,
				executor: def.executor,
				concurrency: def.concurrency,
				running: new AtomicUint(0),
				count: new AtomicUint(0),
				handler: null,
				gearman: null,
			});

			// run one gearman worker per function to be able to control the
			// concurrency of each function separately through the amount of
			// jobs that are grabbed from the server (maxJobs).

			fn.gen = gen;

			if (!fn.gearman) {
				fn.handler = null;
				fn.gearman = Gearman.Client.connect(config.gearman);
			}

			if (fn.executor != def.executor) {
				fn.handler = null;
				fn.gearman.unregisterWorker(name);
				fn.executor = def.executor;
			}

			if (!fn.handler) {
				fn.handler = handler(state, name);
				fn.gearman.registerWorker(name, fn.handler);
			}

			if (fn.concurrency != def.concurrency) {
				fn.gearman.maxJobs = fn.concurrency = def.concurrency;
			}
		}
	}

	for (const [name, fn] of state.functions.entries()) {
		if (fn.gen != gen) {
			fn.handler = null;

			if (fn.gearman) {
				fn.gearman.disconnect();
				fn.gearman = null;
			}

			if (fn.running.zero()) {
				info(`Retiring ${name}; ran ${fn.count.load()} times`);
				state.functions.delete(name);
			}
		}
	}

	info(`Reloaded: ${state.functions.size} functions available`);
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

	const workerOpts = {};
	if (config.worker?.env) workerOpts.env = config.worker.env;
	if (config.worker?.timeout) workerOpts.timeout =
		typeof config.worker.timeout == 'string'
		? ms(config.worker.timeout)
		: config.worker.timeout;
	if (config.worker?.max_buffer) workerOpts.maxBuffer = bytes.parse(config.worker.max_buffer);
	if (config.worker?.user) workerOpts.uid =
		typeof config.worker.user == 'number'
		? config.worker.user
		: userid.uid(config.worker.user);
	if (config.worker?.group) workerOpts.gid =
		typeof config.worker.group == 'number'
		? config.worker.group
		: userid.gid(config.worker.group);

	const state = {
		functions: new Map,
		reloading: new AtomicBool(false),
		quitting: new AtomicBool(false),
		round: new AtomicUint(0),
		workerOpts,
	};

	await reload(config, state);

	// TODO: reload on reload.interval
	// TODO: listen to USR1 and reload
	// TODO: listen to TERM and gracefully shutdown:
	//       - set quitting to true
	//       - call reload repeatedly every second
	//       - shut down when last worker is done
})().catch(err => ohno(err, 1));
