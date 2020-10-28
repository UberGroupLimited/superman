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

async function reload (config) {
	const mysql = knex({
		client: 'mysql2',
		connection: config.mysql,
	});

	const workers = config.workers ||
		await mysql
		.from('pure_gearman_workers')
		.where('active', true)
		.andWhere('concurrency', '>', 0);
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

(async () => {
	const config = await loadConfig();
	rollbar(config.rollbar);

	await reload(config);
})().catch(err => ohno(err, 1));
