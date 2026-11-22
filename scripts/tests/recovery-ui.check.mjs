// Runs the real dialog in headless Edge with mocked native commands.
// From the repo root: node scripts/tests/recovery-ui.check.mjs
import { createServer } from '../../ui/node_modules/vite/dist/node/index.js';
import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';

const repo = fileURLToPath(new URL('../../', import.meta.url));
process.chdir(resolve(repo, 'ui'));
const delay = ms => new Promise(r => setTimeout(r, ms));
const server = await createServer({
    root: resolve(repo, 'ui'), configFile: resolve(repo, 'ui/vite.config.ts'),
    resolve: { dedupe: ['svelte'] },
    server: { host: '127.0.0.1', port: 5191, strictPort: true, fs: { allow: [repo] }, watch: { ignored: ['**/.build-tools/**', '**/target/**'] } },
    plugins: [{
        name: 'recovery-preview',
        configureServer(vite) {
            vite.middlewares.use(async (req, res, next) => {
                if (req.url !== '/__recovery-preview') return next();
                const fixture = '/@fs/' + resolve(repo, 'scripts/tests/RecoveryPreview.svelte').replaceAll('\\', '/');
                const html = await vite.transformIndexHtml(req.url, `<!doctype html><html lang="en" class="dark"><head><meta charset="utf-8"></head><body><script type="module">
                    window.__previewCalls = [];
                    window.__TAURI_INTERNALS__ = { metadata: { currentWindow: { label: 'main' } }, invoke: async (command, args) => { window.__previewCalls.push({command, args}); } };
                    const {mount} = await import('svelte');
                    const {default: Preview} = await import('${fixture}');
                    mount(Preview, {target: document.body});
                </script></body></html>`);
                res.setHeader('Content-Type', 'text/html'); res.end(html);
            });
        }
    }]
});
await server.listen();
const browser = spawn('C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe', [
    '--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
    '--remote-debugging-port=9231', `--user-data-dir=${resolve(repo, '.build-tools/edge-recovery-' + Date.now())}`, 'about:blank'
], { windowsHide: true, stdio: 'ignore' });
let ws;
try {
    let target;
    for (let i = 0; i < 150; i++) {
        try { target = await (await fetch('http://127.0.0.1:9231/json/new?about:blank', { method: 'PUT' })).json(); break; }
        catch { await delay(100); }
    }
    assert(target, 'Edge debug endpoint did not start');
    ws = new WebSocket(target.webSocketDebuggerUrl);
    await new Promise((ok, no) => { ws.onopen = ok; ws.onerror = no; });
    let id = 0; const pending = new Map();
    ws.onmessage = e => {
        const msg = JSON.parse(e.data);
        if (msg.method === 'Runtime.exceptionThrown') console.error(JSON.stringify(msg.params.exceptionDetails));
        if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error') console.error(JSON.stringify(msg.params.args));
        if (!msg.id) return;
        const call = pending.get(msg.id); pending.delete(msg.id);
        msg.error ? call.reject(new Error(JSON.stringify(msg.error))) : call.resolve(msg.result);
    };
    const cdp = (method, params = {}) => new Promise((resolve, reject) => {
        pending.set(++id, { resolve, reject }); ws.send(JSON.stringify({ id, method, params }));
    });
    const evaluate = async expression => {
        const r = await cdp('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
        if (r.exceptionDetails) throw new Error(JSON.stringify(r.exceptionDetails));
        return r.result.value;
    };
    const eventually = async expression => {
        for (let i = 0; i < 200; i++) { if (await evaluate(expression)) return; await delay(100); }
        throw new Error('Timed out: ' + expression + '\n' + await evaluate('document.body.innerText'));
    };
    const click = async label => {
        await evaluate(`Array.from(document.querySelectorAll('button')).find(b => b.textContent.trim() === ${JSON.stringify(label)}).click()`);
        await delay(150);
    };
    await cdp('Page.enable');
    await cdp('Runtime.enable');
    const navigation = await cdp('Page.navigate', { url: 'http://127.0.0.1:5191/__recovery-preview' });
    assert(!navigation.errorText, navigation.errorText);
    await eventually('!!window.__recoveryPreview');
    assert.equal(await evaluate('!!document.querySelector("[role=dialog]")'), false);
    await evaluate('window.__recoveryPreview.set({status:"connecting", recoveryDisconnected:true})');
    await eventually('document.body.innerText.includes("Host and invite friends")');
    assert((await evaluate('document.body.innerText')).includes('current track and upcoming playlist are kept'));
    assert.equal(await evaluate('window.__previewCalls.length'), 0, 'Detecting a loss must not start hosting or move rooms');
    await click('Keep waiting');
    await eventually('!document.querySelector("[role=dialog]")');
    await evaluate('window.__recoveryPreview.set({recoveryInviter:"Alex"})');
    await eventually('document.body.innerText.includes("Alex has a replacement room ready")');
    assert.equal(await evaluate('window.__previewCalls.length'), 0, 'An invitation alone must not move rooms');
    await click('Join replacement room');
    assert.equal(await evaluate('window.__previewCalls.filter(c => c.command === "lt_accept_recovery").length'), 1);
    assert.equal(await evaluate('window.__previewCalls.filter(c => c.command === "lt_host_recovery").length'), 0);
    await evaluate('window.__recoveryPreview.set({recoveryInviter:null})');
    await click('Host and invite friends');
    assert.equal(await evaluate('window.__previewCalls.filter(c => c.command === "lt_host_recovery").length'), 1);
    console.log('PASS outage opens the closed dialog; waiting stays put; joining and hosting require explicit clicks');
    await cdp('Browser.close');
} finally {
    ws?.close(); browser.kill(); await server.close();
}
