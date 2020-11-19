#!/usr/bin/env node
'use strict';
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
import pino from 'pino';
import readline from 'readline';
import { env } from 'process';
import { promises as fs, readFileSync } from 'fs';
import { promisify } from 'util';

const log = pino({
	redact: ['rollbar.accessToken'],
});
log.on('level-change', (lvl, val, prevLvl, prevVal) => {
	if (val != prevVal) {
		log.trace({ loglevel: lvl }, 'logging level changed');
	}
});

if (env.SUPERMAN_VERBOSE) log.level = env.SUPERMAN_VERBOSE;

export function sleep(ms) {
	log.trace({ duration: ms }, 'sleeping');
	return new Promise((resolve) => setTimeout(resolve, ms))
}

async function loadConfig(stdin = false) {
	let contents;
	if (stdin) {
		log.debug('reading config from stdin');
		contents = readFileSync(process.stdin.fd);
	} else {
		log.trace('looking for config');
		const filename = (await glob([
			'./superman.(json|toml)',
			'/apps/superman/live/superman.(json|toml)',
			'/etc/superman.(json|toml)',
		]))[0];

		if (!filename) throw new Error('Cannot find a superman configuration file');
		log.debug({ filename }, 'found config');

		contents = await fs.readFile(filename);
	}

	if (!contents) throw new Error('could not read config');
	contents = contents.toString();
	log.trace({ contents }, 'read config');

	try {
		const config = TOML.parse(contents);
		log.debug({ config }, 'parsed toml config');
		return config;
	} catch (err) {
		log.trace({ err }, 'toml parse error');

		const config = JSON.parse(contents);
		log.debug({ config }, 'parsed json config');
		return config;
	}
}

function handler (state, name) {
	const hlog = log.child({ fn: name });

	hlog.trace('created handler');
	return (task) => (async () => {
		const tlog = hlog.child({ jobid: task.jobid });
		tlog.debug('got task');

		const fn = state.functions.get(name);
		tlog.trace('loaded function');

		const running = fn.running.incr();
		const count = fn.count.incr();
		tlog.info({ running, count, concurrency: fn.concurrency }, 'running function');

		const workload = Buffer.from(task.payload);
		tlog.debug({ length: workload.length }, 'obtained payload');

		const tmpname = `${name.replace(/\W+/g, '')}-${task.uniqueid}-workdir`;
		tlog.trace({ tmpname }, 'generated tmpdir name');
		const workdir = await fs.mkdtemp(path.join(os.tmpdir(), tmpname));
		tlog.debug({ workdir }, 'created workdir');

		async function stat(statname, runtime = null) {
			if (!state.influx?.database) return;

			let [ns, method] = name.split('::', 2);
			if (!method) { method = ns; ns = ''; }

			const point = {
				measurement: `${state.influx.prefix}${statname}`,
				tags: { ns, method },
				fields: { value: 1, running, count },
			};

			if (runtime !== null) {
				// influx runtime is in secs, js's in ms
				point.fields.runtime = runtime / 1000;
			}

			tlog.trace({ point }, 'send influx stat');
			return state.influx.writePoints([point]);
		}

		const start = +new Date;
		stat('start').catch(ohno);

		try {
			tlog.trace('wrapping process spawn');
			await new Promise((resolve, reject) => {
				tlog.debug({ executor: fn.executor, args: [name, task.uniqueid, workload.length] }, 'spawning');
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
					tlog.trace({ chunk }, 'read stderr');
					stderr += chunk.toString();
					process.stderr.write(chunk);
				});

				tlog.trace('attaching readline to stdout');
				const reader = readline.createInterface({
					input: run.stdout,
					terminal: false,
				});
				reader.on('line', (line) => {
					tlog.trace({ line }, 'read stdout');
					try {
						const data = JSON.parse(line);
						switch (data.type) {
							case 'complete':
								tlog.debug('stdout got completion');
								tlog.trace({ data }, 'completion');
								delete data.type;
								task.end(JSON.stringify(data));
								run.removeAllListeners();
								stat(data.error ? 'errored' : 'finished', new Date - start).catch(ohno);
								resolve();
							break;

							case 'update':
								tlog.debug('stdout got update');
								tlog.trace({ data }, 'update');
								task.update(JSON.stringify(data.data));
							break;

							case 'progress':
								const { numerator, denominator } = data;
								tlog.debug({ numerator, denominator }, 'stdout got progress');
								task.progress(numerator, denominator);
							break;

							case 'print':
								tlog.debug('stdout got print');
								process.stderr.write(data.content);
							break;

							default:
								throw new Error(`unsupported event type: ${data.type}`);
						}
					} catch (err) {
						ohno(err);
					}
				});

				run.on('error', (err) => {
					tlog.debug({ err }, 'got error event');
					run.removeAllListeners();
					reject(err);
				});

				run.on('exit', (code) => {
					tlog.trace({ code }, 'got exit event');
					if (code === 0) {
						tlog.debug('exit event with success, resolving');
						run.removeAllListeners();
						resolve();
					} else {
						tlog.debug('exit event with failure, rejecting');
						run.removeAllListeners();
						reject(new Error(`worker executor exited with ${code}\n${stderr}`));
					}
				});

				tlog.trace('injecting workload on stdin');
				run.stdin.end(workload);
			});
		} catch (err) {
			ohno(err);
			tlog.debug('sending gearman exception');
			task.error(err);
		} finally {
			tlog.trace('decr running');
			fn.running.decr();

			tlog.debug({ workdir }, 'deleting workdir');
			await fs.rmdir(workdir, {
				maxRetries: 20,
				recursive: true,
			});

			stat('finished', new Date - start).catch(ohno);
		}
	})().catch(ohno);
}

async function executorCaps(executor) {
	try {
		const { stdout } = await promisify(cp.execFile)(executor, ['--caps']);
		const { caps } = JSON.parse(stdout);
		if (!Array.isArray(caps)) return [];
		return caps;
	} catch (_) {
		return [];
	}
}

async function reload (config, state) {
	if (state.reloading.cas(false, true)) {
		log.warn('not reloading while reloading');
		return;
	}

	const gen = state.round.incr();
	const rlog = log.child({ gen });
	rlog.info('reloading');

	if (state.quitting.load()) {
		rlog.warn('reloading while quitting (normal during graceful shutdown)');
	} else {
		let functions;
		if (config.functions?.length) {
			rlog.debug('functions are from config');
			functions = config.functions;
		} else {
			const connection = Object.assign({}, config.mysql);
			delete connection.table;
			delete connection.client;

			rlog.trace({ connection }, 'connecting to mysql');
			const mysql = knex({
				client: config.mysql?.client || 'mysql2',
				connection,
			});

			const table = config.mysql?.table || 'gearman_functions';
			rlog.debug({ table }, 'querying mysql');
			functions = await mysql
				.from(table)
				.where('active', true)
				.andWhere('concurrency', '>', 0);
		}
		rlog.debug({ fns: functions.length }, 'got functions');

		for (const [index, def] of functions.entries()) {
			rlog.trace({ def }, 'loading function def');

			if (!(def.name || (def.ns && def.method))) {
				ohno(new Error(`empty name or ns+method for id/index ${def.id || index}`));
				continue;
			}

			// the executor provides a simple way to get some work done on
			// select nodes, such that if the executor (= worker program)
			// doesn't exist or isn't executable, the definition will be silently
			// skipped here. so resist the urge to make this an error e hoa!
			rlog.trace('checking executor supports single');
			if (!(await executorCaps(def.executor)).includes('single')) {
				rlog.debug('skipping as executor isn’t available');
				continue;
			}

			const name = def.name || (def.ns + '::' + def.method);
			const dlog = rlog.child({ defname: name });
			dlog.trace('constructed fn name');

			dlog.trace('loading fn entry');
			let fn = state.functions.get(name);
			if (!fn) {
				dlog.debug('creating new fn entry');
				state.functions.set(name, fn = {
					gen,
					executor: def.executor,
					concurrency: def.concurrency,
					running: new AtomicUint(0),
					count: new AtomicUint(0),
					handler: null,
					gearman: null,
				});
			}

			// run one gearman worker per function to be able to control the
			// concurrency of each function separately through the amount of
			// jobs that are grabbed from the server (maxJobs).

			dlog.trace({ oldgen: fn.gen }, 'setting gen');
			fn.gen = gen;

			if (!fn.gearman) {
				dlog.debug('no gearman, initialising');
				fn.handler = null;
				fn.gearman = Gearman.Client.connect({
					...config.gearman,
					maxJobs: def.concurrency,
				});
			}

			if (fn.executor != def.executor) {
				dlog.debug('different executor, resetting handler');
				fn.handler = null;
				dlog.trace('unregistering worker');
				fn.gearman.unregisterWorker(name);
				fn.executor = def.executor;
			}

			if (!fn.handler) {
				dlog.debug('no handler, initialising');
				fn.handler = handler(state, name);
				dlog.trace('registering worker');
				fn.gearman.registerWorker(name, fn.handler);
			}

			if (fn.concurrency != def.concurrency) {
				dlog.debug({ concurrency: { old: fn.concurrency, fut: def.concurrency } }, 'different concurrency, amending');
				fn.gearman.maxJobs = fn.concurrency = def.concurrency;
			}
		}
	}

	rlog.debug('garbage-collecting old gen functions');
	for (const [name, fn] of state.functions.entries()) {
		const flog = rlog.child({ fn: name });
		flog.trace('examining fn for gc');
		if (fn.gen != gen) {
			flog.debug({ fngen: fn.gen }, 'found function to gc');

			flog.trace('clearing handler');
			fn.handler = null;

			if (fn.gearman) {
				flog.debug('disconnecting gearman');
				fn.gearman.disconnect();
				fn.gearman = null;
			}

			if (fn.running.zero()) {
				flog.info({ ran: fn.count.load() }, 'retiring function');
				state.functions.delete(name);
			} else {
				flog.debug({ running: fn.running.load() }, 'function is still running');
			}
		}
	}

	rlog.info({ fns: state.functions.size }, 'reloaded');
	rlog.trace('set reloading atomic to false');
	state.reloading.store(false);
}

let rollbarInstance = null;
function rollbar (token = null) {
	if (rollbarInstance !== null) return rollbarInstance;
	if (!token) {
		log.warn('no rollbar token');
		return rollbarInstance = false;
	}

	const opts = {
		accessToken: token,
		environment: env.NODE_ENV || 'development',
		captureUncaught: true,
		captureUnhandledRejections: true,
	};

	log.debug({ rollbar: opts }, 'creating rollbar instance');
	return rollbarInstance = new Rollbar(opts);
}

function ohno (err, exit = false) {
	log.error({ err }, err.toString());

	const roll = rollbar();
	if (roll) {
		log.trace('erroring to rollbar');
		roll.error(err);
	}

	if (exit !== false) {
		log.fatal({ exit }, 'exeunt');
		process.exit(exit);
	}
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
		return !!Atomics.store(this.ta, 0, +!!value);
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
		return Atomics.store(this.ta, 0, +value);
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
	log.debug({ args: process.argv }, 'init');
	const config = await loadConfig(process.argv[process.argv.length - 1] == '-');

	if (config.verbose) {
		log.trace({ loglevel: config.verbose }, 'setting log level');
		log.level = config.verbose;
	}

	log.trace('configuring rollbar');
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
	log.debug({ workerOpts }, 'worker options');

	log.trace('initialising state');
	const state = {
		functions: new Map,
		reloading: new AtomicBool(false),
		quitting: new AtomicBool(false),
		round: new AtomicUint(0),
		workerOpts,
		reload: {},
	};

	if (state.influx?.database) {
		log.trace('initialising influx');
		const incon = Object.assign({}, config.influx);
		const prefix = incon.prefix || 'gearman_ucworker_';
		delete incon.prefix;

		state.influx = new Influx.InfluxDB({
			schema: [
				{
					measurement: `${prefix}start`,
					fields: {
						value: Influx.FieldType.INTEGER,
						running: Influx.FieldType.INTEGER,
						count: Influx.FieldType.INTEGER,
					},
					tags: ['ns', 'method'],
				},
				{
					measurement: `${prefix}complete`,
					fields: {
						value: Influx.FieldType.INTEGER,
						running: Influx.FieldType.INTEGER,
						count: Influx.FieldType.INTEGER,
						runtime: Influx.FieldType.FLOAT,
					},
					tags: ['ns', 'method'],
				},
				{
					measurement: `${prefix}errored`,
					fields: {
						value: Influx.FieldType.INTEGER,
						running: Influx.FieldType.INTEGER,
						count: Influx.FieldType.INTEGER,
						runtime: Influx.FieldType.FLOAT,
					},
					tags: ['ns', 'method'],
				},
				{
					measurement: `${prefix}finished`,
					fields: {
						value: Influx.FieldType.INTEGER,
						running: Influx.FieldType.INTEGER,
						count: Influx.FieldType.INTEGER,
						runtime: Influx.FieldType.FLOAT,
					},
					tags: ['ns', 'method'],
				},
			],
			...incon,
		});
		state.influx.prefix = prefix;
	}

	log.trace('initial reload');
	await reload(config, state);

	if (config.interval) {
		log.trace({ interval: config.interval }, 'parsing reload interval');
		const interval =
			typeof config.interval == 'string'
			? ms(interval)
			: interval;

		log.info({ interval }, 'reloading on interval');
		state.reload.interval = setInterval(() => reload(config, state).catch(ohno), interval);
	}

	log.trace({ signal: 'SIGUSR2' }, 'installing signal handler');
	process.on('SIGUSR2', () => {
		log.debug({ signal: 'SIGUSR2' }, 'received reload signal');
		reload(config, state).catch(ohno);
	});

	async function shutdown (signal) {
		log.debug({ signal }, 'received shutdown signal');

		log.trace('set quitting atomic to true');
		state.quitting.store(true);

		if (state.reload.interval) {
			log.trace('clear reload interval');
			clearInterval(state.reload.interval);
		}

		log.debug('starting shutdown loop');
		while (true) {
			await reload(config, state);

			if (state.functions.size == 0) {
				log.info('all finished, shutting down');
				process.exit(0);
				return;
			}

			log.info({ fns: state.functions.size }, 'functions remaining');
			await sleep(1000);
		}
	}

	log.trace({ signal: 'SIGINT' }, 'installing signal handler');
	process.on('SIGINT', () => shutdown('SIGINT').catch((err) => ohno(err, 5)));

	log.trace({ signal: 'SIGTERM' }, 'installing signal handler');
	process.on('SIGTERM', () => shutdown('SIGTERM').catch((err) => ohno(err, 6)));
})().catch(err => ohno(err, 1));
