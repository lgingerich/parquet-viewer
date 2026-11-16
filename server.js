// Tiny static server: serves public/, maps /vendor/* to node_modules/*, and exposes samples/*.parquet.
// Files you "upload" never leave the browser; they are read locally via File.slice().
import { createServer } from 'node:http'
import { readFile } from 'node:fs/promises'
import { watch } from 'node:fs'
import { extname, join, normalize, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('.', import.meta.url))
const port = Number(process.env.PORT) || 8765
const dev = !!process.env.DEV // `npm run dev`: live reload
const types = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml', '.wasm': 'application/wasm' }

// url prefix -> [directory, allowed extensions]
const mounts = [
  ['/vendor/', join(root, 'node_modules'), null],
  ['/samples/', join(root, 'samples'), ['.parquet']],
  ['/', join(root, 'public'), ['.html', '.js', '.css', '.svg']],
]

// ── dev mode: tell open browser tabs to reload when files change ──
// Front-end files are read from disk on every request, so a browser refresh already picks up edits;
// this just does the refresh for you. (`node --watch` restarts the server itself when server.js changes,
// and the page reloads once it reconnects.)
const clients = new Set()
const reloadClient = `<script>(() => { let down = false; const es = new EventSource('/__reload'); es.onmessage = () => location.reload(); es.onerror = () => { down = true }; es.onopen = () => { if (down) location.reload() } })()</script>`
if (dev) {
  let timer
  for (const dir of ['public', 'samples']) {
    watch(join(root, dir), { recursive: true }, () => {
      clearTimeout(timer)
      timer = setTimeout(() => clients.forEach(res => res.write('data: reload\n\n')), 80)
    })
  }
}

createServer(async (req, res) => {
  try {
    let path = decodeURIComponent(new URL(req.url, 'http://x').pathname)
    if (dev && path === '/__reload') {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' }).write(': connected\n\n')
      clients.add(res); req.on('close', () => clients.delete(res))
      return
    }
    if (path === '/') path = '/index.html'
    const [prefix, dir, exts] = mounts.find(([p]) => path.startsWith(p))
    const file = resolve(dir, normalize(path.slice(prefix.length)))
    if (!file.startsWith(dir + sep) || (exts && !exts.includes(extname(file)))) { res.writeHead(404).end('not found'); return }
    let body = await readFile(file)
    if (dev && path === '/index.html') body = Buffer.from(body.toString().replace('</body>', reloadClient + '</body>'))
    res.writeHead(200, { 'content-type': types[extname(file)] || 'application/octet-stream', 'cache-control': 'no-cache' }).end(body)
  } catch {
    res.writeHead(404).end('not found')
  }
}).listen(port, '127.0.0.1', () => console.log(`Parquet viewer: http://localhost:${port}${dev ? '  (dev mode: live reload)' : ''}`))
