import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import { isNewer, isPrerelease } from './version.ts';

// Run the actual updater with mocked IPC/network boundaries. A fork must never
// fetch or offer an upstream installer, including through the beta/fallback paths.
let identifier = 'com.limusic.plus';
let networkCalls = 0;
let plusChecks = 0;
let plusResponse = { configured: false, release: null };
let openedUrl = '';
const { version } = JSON.parse(readFileSync(new URL('../../../src-tauri/tauri.conf.json', import.meta.url), 'utf8'));
const stubs = {
	$state: (value) => value,
	check: async () => { networkCalls++; return null; },
	Update: class {},
	relaunch: async () => {},
	toast: { error: () => {} },
	t: (key) => key,
	friendlyNetError: (error) => error,
	canSelfUpdate: async () => false,
	checkBetaUpdate: async () => { networkCalls++; return null; },
	checkPlusRelease: async () => { plusChecks++; return plusResponse; },
	getSettings: async () => ({ update_banner: 'true', update_channel: 'beta' }),
	openExternal: async (url) => { openedUrl = url; },
	releaseNotes: async () => { networkCalls++; return []; },
	isNewer,
	isPrerelease,
	getVersion: async () => version,
	getIdentifier: async () => identifier
};
const source = readFileSync(new URL('./updater.svelte.ts', import.meta.url), 'utf8')
	.replace(/^import .*;\r?$/gm, '').replace(/\bexport /g, '');
const factory = new Function(...Object.keys(stubs), stripTypeScriptTypes(source) +
	'\nreturn { checkForUpdatesQuiet, checkForUpdatesInteractive, recheckForUpdates, installUpdate, openDownloadPage, updateState };');
const updater = factory(...Object.values(stubs));
await updater.checkForUpdatesQuiet();
await updater.recheckForUpdates();
assert.deepEqual(await updater.checkForUpdatesInteractive(), {
	message: 'settings.about.plus_installer_updates', error: false
});
await updater.installUpdate();
await updater.openDownloadPage();
assert.equal(openedUrl, '', 'an unconfigured Plus build has no download page');
assert.equal(plusChecks, 3);

const [major, minor, patch] = version.split('.').map(Number);
const newer = [major, minor, patch + 1].join('.');
const downloadUrl = `https://github.com/example/limusic-plus/releases/tag/v${newer}`;
plusResponse = { configured: true, release: { version: newer, downloadUrl } };
await updater.checkForUpdatesQuiet();
assert.deepEqual(updater.updateState.available, { version: newer, downloadUrl, rollback: false });
assert.equal(updater.updateState.canInstall, false);
await updater.openDownloadPage();
assert.equal(openedUrl, downloadUrl);
await updater.installUpdate();

plusResponse.release = { version, downloadUrl };
assert.deepEqual(await updater.checkForUpdatesInteractive(), {
	message: 'settings.about.up_to_date', error: false
});
assert.equal(updater.updateState.available, null);
assert.equal(networkCalls, 0, 'fork updater must not contact or install upstream releases');
identifier = 'com.limusic.desktop';
await updater.checkForUpdatesQuiet();
assert.equal(networkCalls, 1, 'original build keeps its beta update check');
console.log('ok — fork reads only its own release feed and offers newer installers');
