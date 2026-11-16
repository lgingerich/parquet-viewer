import { plain } from './util.js'

export const isCont = v => v !== null && typeof v === 'object' && !(v instanceof Date) && !(v instanceof Uint8Array)
const entriesOf = v => Array.isArray(v) ? v.map((x, i) => [i, x]) : Object.entries(v)

function jsonTree(data, { query = '', openDepth = 2 } = {}) {
  const q = query.trim().toLowerCase()
  const memo = new Map()
  const leafText = v => v === null || v === undefined ? 'null' : typeof v === 'string' ? v : plain(v)
  const matchSelf = (k, v) => q && ((typeof k === 'string' && k.toLowerCase().includes(q)) || (!isCont(v) && leafText(v).toLowerCase().includes(q)))
  const descMatch = v => {
    if (memo.has(v)) return memo.get(v)
    let r = false
    for (const [ck, cv] of entriesOf(v)) if (matchSelf(ck, cv) || (isCont(cv) && descMatch(cv))) { r = true; break }
    memo.set(v, r); return r
  }
  const visible = (k, v) => !q || matchSelf(k, v) || (isCont(v) && descMatch(v))

  let matches = 0
  const span = (cls, text) => { const e = document.createElement('span'); if (cls) e.className = cls; if (text != null) e.textContent = text; return e }
  const hl = (cls, text) => {
    const e = span(cls)
    if (!q) { e.textContent = text; return e }
    const lower = text.toLowerCase(); let i = 0, j
    while ((j = lower.indexOf(q, i)) !== -1) {
      e.append(text.slice(i, j)); const m = document.createElement('mark'); m.textContent = text.slice(j, j + q.length); e.append(m); matches++; i = j + q.length
    }
    e.append(text.slice(i)); return e
  }

  // returns { el, setOpen(open, deep), isCont }
  function node(k, v, depth, last, path) {
    const el = document.createElement('div')
    const line = document.createElement('div'); line.className = 'line'; line.title = path || '$'
    const caret = span('caret'); line.append(caret)
    if (k !== null) { line.append(typeof k === 'number' ? span('p', k + ': ') : hl('k', JSON.stringify(k)), typeof k === 'number' ? '' : span('p', ': ')) }
    const comma = last ? '' : ','
    el.append(line)
    if (!isCont(v)) {
      let cls = 's', text
      if (v === null || v === undefined) { cls = 'z'; text = 'null' }
      else if (typeof v === 'string') text = JSON.stringify(v)
      else if (typeof v === 'number' || typeof v === 'bigint') { cls = 'n'; text = String(v) }
      else if (typeof v === 'boolean') { cls = 'b'; text = String(v) }
      else text = JSON.stringify(plain(v))
      line.append(hl(cls, text), span('p', comma))
      return { el, setOpen() {}, isCont: false }
    }
    const arr = Array.isArray(v), ents = entriesOf(v)
    const [o, c] = arr ? ['[', ']'] : ['{', '}']
    el.className = 'cont'
    line.append(span('p', o), span('ell p', ents.length ? ' … ' : ''), span('p ell', c + comma), span('sum', `${ents.length} ${arr ? 'item' : 'key'}${ents.length === 1 ? '' : 's'}`))
    const kids = document.createElement('div'); kids.className = 'kids'
    const close = document.createElement('div'); close.className = 'close p'; close.textContent = c + comma
    el.append(kids, close)
    let built = false, childNodes = []
    const build = () => {
      built = true
      const vis = ents.filter(([ck, cv]) => visible(ck, cv))
      vis.forEach(([ck, cv], i) => {
        const n = node(ck, cv, depth + 1, i === vis.length - 1, path + (arr ? `[${ck}]` : /^[A-Za-z_]\w*$/.test(ck) ? '.' + ck : `["${ck}"]`))
        childNodes.push(n); kids.append(n.el)
      })
      if (!vis.length) kids.append(Object.assign(span('empty'), { textContent: '(empty)' }))
    }
    const self = {
      el, isCont: true,
      setOpen(open, deep) {
        if (open && !built) build()
        el.classList.toggle('open', open)
        if (deep) childNodes.forEach(n => n.setOpen(open, true))
      },
      toggle() { self.setOpen(!el.classList.contains('open'), false) },
    }
    line.addEventListener('click', e => { if (!window.getSelection().toString()) { e.shiftKey ? self.setOpen(!el.classList.contains('open'), true) : self.toggle() } })
    // while searching, open every container that leads to a match; otherwise open to openDepth
    self.setOpen(q ? ents.some(([ck, cv]) => visible(ck, cv)) : depth < openDepth, false)
    return self
  }
  const root = node(null, data, 0, true, '$')
  const wrap = document.createElement('div'); wrap.className = 'jt'; wrap.append(root.el)
  if (q && !matches) wrap.append(Object.assign(span('empty'), { textContent: 'No matches.' }))
  return { el: wrap, root, matches }
}

// Toolbar + tree: search, expand/collapse all, copy.
export function mountJson(host, data, { toolbar = true, openDepth = 2 } = {}) {
  const bar = document.createElement('div'); bar.className = 'jtbar'
  const holder = document.createElement('div')
  host.append(bar, holder)
  let tree
  const render = (query = '') => {
    tree = jsonTree(data, { query, openDepth })
    holder.replaceChildren(tree.el)
    count.textContent = query.trim() ? `${tree.matches} match${tree.matches === 1 ? '' : 'es'}` : ''
  }
  const btn = (label, fn) => { const b = document.createElement('button'); b.className = 'btn'; b.textContent = label; b.onclick = fn; return b }
  const search = document.createElement('input'); search.type = 'search'; search.className = 'input'; search.placeholder = 'Search keys & values…'
  const count = document.createElement('span'); count.className = 'count'
  let timer
  search.oninput = () => { clearTimeout(timer); timer = setTimeout(() => render(search.value), 150) }
  const copy = btn('Copy JSON', async () => {
    await navigator.clipboard.writeText(JSON.stringify(data, (_, v) => typeof v === 'bigint' ? v.toString() : v instanceof Uint8Array ? plain(v) : v, 2))
    copy.textContent = 'Copied ✓'; setTimeout(() => copy.textContent = 'Copy JSON', 1200)
  })
  bar.append(
    btn('Expand all', () => tree.root.setOpen(true, true)),
    btn('Collapse all', () => { tree.root.setOpen(false, true); tree.root.setOpen(true, false) }),
    search, count, copy,
  )
  if (!toolbar) bar.hidden = true
  const tipEl = document.createElement('p'); tipEl.className = 'note'
  tipEl.textContent = 'Click a row to expand/collapse it · Shift-click to expand/collapse everything beneath it · hover a row for its JSON path.'
  host.insertBefore(tipEl, holder)
  render()
}
