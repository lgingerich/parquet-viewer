import { parquetMetadataAsync, parquetReadObjects, readColumnIndex, readOffsetIndex } from '/vendor/hyparquet/src/index.js'
import { parquetHeader } from '/vendor/hyparquet/src/column.js'
import { compressors } from '/vendor/hyparquet-compressors/src/index.js'
import { $, esc, num, bytes, fmtInt, plain, fmtVal } from './util.js'
import { ENC_COLOR, CODEC_COLOR, STRUCT_COLOR, columnColor, primaryEncoding, encPill, codecPill } from './colors.js'
import { mountJson, isCont } from './jsontree.js'
import { renderGrid, updateCell, fitHeads } from './grid.js'

let state = null // everything about the currently open file

const TABS = [
  ['guide', 'Example guide'], ['schema', 'Schema'], ['rowgroups', 'Row groups'], ['chunk', 'Pages'],
  ['data', 'Data'], ['kv', 'Key-value'], ['footer', 'Footer (JSON)'],
]

// ───────────── loading ─────────────
// Reads only the byte ranges it needs via File.slice(), so big files are fine.
const asyncBuffer = file => ({ byteLength: file.size, slice: (s, e) => file.slice(s, e).arrayBuffer() })

async function load(file) {
  $('err').hidden = true
  try {
    if (file.size < 12) throw new Error('File is too small to be Parquet.')
    const buf = asyncBuffer(file)
    const head = new DataView(await buf.slice(0, 4))
    const tail = new DataView(await buf.slice(file.size - 8, file.size))
    if (head.getUint32(0, true) !== 0x31524150 || tail.getUint32(4, true) !== 0x31524150) throw new Error('Not a Parquet file (missing PAR1 magic bytes).')
    const footerLen = tail.getUint32(0, true)
    const meta = await parquetMetadataAsync(buf)

    const colNames = []
    meta.row_groups.forEach(rg => rg.columns.forEach(c => {
      const n = c.meta_data.path_in_schema.join('.')
      if (!colNames.includes(n)) colNames.push(n)
    }))
    const colColor = Object.fromEntries(colNames.map((n, i) => [n, columnColor(i, colNames.length)]))
    state = {
      file, buf, meta, footerLen, footerStart: file.size - 8 - footerLen, colNames, colColor,
      selected: null, tab: 'schema', rg: 0, colorBy: 'column', view: 'logical', widthMode: 'equal', pageCache: {}, dataStart: 0,
      guide: readGuide(meta), schemaByPath: schemaPaths(meta),
    }
    state.segs = buildSegments()
    if (state.guide) state.tab = 'guide'
    applyHash()

    $('landing').hidden = true; $('app').hidden = false; $('actions').hidden = false
    $('fname').textContent = file.name; $('fsize').textContent = bytes(file.size); $('filechip').hidden = false
    $('ex2').classList.toggle('on', file.name === 'example_v2.parquet')
    $('ex1').classList.toggle('on', file.name === 'example_v1.parquet')
    renderStats(); renderControls(); renderMap(); renderTabs(); renderPane()
    loadGridPages()
    scrollTo(0, 0)
  } catch (e) {
    console.error(e)
    showErr(new Error('Could not read file: ' + e.message))
  }
}
// Back to the start page (the logo in the header). Plain clicks stay in-page; modified clicks open "/" normally.
function goHome() {
  state = null
  $('app').hidden = true; $('actions').hidden = true; $('filechip').hidden = true; $('err').hidden = true
  $('landing').hidden = false
  history.replaceState(null, '', location.pathname) // drop any #tab=… deep link so it doesn't leak into the next file
  scrollTo(0, 0)
}
$('home').addEventListener('click', e => { if (e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey && !e.altKey) { e.preventDefault(); goHome() } })

function showErr(e) { $('err').textContent = e.message; $('err').hidden = false }

// Deep links, e.g. /#tab=chunk&rg=0&col=8 or /#tab=data (applied when a file is opened)
function applyHash() {
  const h = new URLSearchParams(location.hash.slice(1))
  const tab = h.get('tab')
  if (tab && TABS.some(([k]) => k === tab)) state.tab = tab
  if (h.has('rg') && h.has('col')) state.selected = { rg: +h.get('rg'), col: +h.get('col') }
}

// ───────────── byte layout ─────────────
function chunkStart(m) {
  const d = num(m.dictionary_page_offset ?? 0)
  return d > 0 && d < num(m.data_page_offset) ? d : num(m.data_page_offset)
}
const chunkName = m => m.path_in_schema.join('.')

function buildSegments() {
  const { meta, file, footerStart, footerLen } = state
  const segs = [{ start: 0, len: 4, label: 'PAR1 magic (header)', kind: 'magic' }]
  meta.row_groups.forEach((rg, ri) => rg.columns.forEach((c, ci) => {
    const m = c.meta_data, name = chunkName(m)
    segs.push({ start: chunkStart(m), len: num(m.total_compressed_size), kind: 'chunk', rg: ri, col: ci, name, label: `Row group ${ri} · ${name}` })
    if (m.bloom_filter_offset != null) segs.push({ start: num(m.bloom_filter_offset), len: m.bloom_filter_length || 0, kind: 'bloom', label: `Bloom filter · RG ${ri} · ${name}` })
    if (c.column_index_offset != null) segs.push({ start: num(c.column_index_offset), len: c.column_index_length || 0, kind: 'colidx', label: `Column index · RG ${ri} · ${name}` })
    if (c.offset_index_offset != null) segs.push({ start: num(c.offset_index_offset), len: c.offset_index_length || 0, kind: 'offidx', label: `Offset index · RG ${ri} · ${name}` })
  }))
  segs.push({ start: footerStart, len: footerLen, kind: 'footer', label: 'Footer (FileMetaData, Thrift-encoded)' })
  segs.push({ start: file.size - 8, len: 4, kind: 'footerlen', label: 'Footer length (4-byte little-endian)' })
  segs.push({ start: file.size - 4, len: 4, kind: 'magic', label: 'PAR1 magic (trailer)' })
  segs.sort((a, b) => a.start - b.start)
  const out = []; let pos = 0
  for (const s of segs) {
    if (s.len <= 0) continue
    if (s.start > pos) out.push({ start: pos, len: s.start - pos, kind: 'gap', label: 'Other / unaccounted bytes' })
    out.push(s); pos = Math.max(pos, s.start + s.len)
  }
  return out
}

function segColor(s) {
  if (s.kind !== 'chunk') return STRUCT_COLOR[s.kind]
  const m = state.meta.row_groups[s.rg].columns[s.col].meta_data
  if (state.colorBy === 'codec') return CODEC_COLOR[m.codec] || '#94a3b8'
  if (state.colorBy === 'encoding') return ENC_COLOR[primaryEncoding(m)] || '#94a3b8'
  return state.colColor[s.name]
}

// ───────────── summary + file map ─────────────
function renderStats() {
  const { meta, file, footerLen } = state
  const sum = f => meta.row_groups.reduce((a, rg) => a + rg.columns.reduce((b, c) => b + num(f(c.meta_data)), 0), 0)
  const comp = sum(m => m.total_compressed_size), unc = sum(m => m.total_uncompressed_size)
  const chunks = meta.row_groups.reduce((a, rg) => a + rg.columns.length, 0)
  const items = [
    ['File size', bytes(file.size)], ['Rows', fmtInt(meta.num_rows)], ['Row groups', meta.row_groups.length],
    ['Columns', state.colNames.length, 'leaf columns'], ['Column chunks', fmtInt(chunks)], ['Footer', bytes(footerLen), `${(footerLen / file.size * 100).toFixed(1)}% of file`],
    ['Compression', comp ? (unc / comp).toFixed(2) + '×' : '—', comp ? `${bytes(unc)} → ${bytes(comp)}` : ''],
  ]
  $('stats').innerHTML = items.map(([k, v, s]) => `<div class="stat"><div class="k">${k}</div><div class="v">${esc(v)}</div>${s ? `<div class="s">${esc(s)}</div>` : ''}</div>`).join('')
  $('byline').textContent = `Written by ${meta.created_by || 'unknown writer'} · Parquet format version ${meta.version}`
}

function renderControls() {
  const on = (id, attr, val) => $(id).querySelectorAll('button').forEach(b => b.classList.toggle('on', b.dataset[attr] === val))
  on('colorby', 'mode', state.colorBy); on('viewctl', 'view', state.view); on('widthmode', 'w', state.widthMode)
}
for (const [id, attr, key] of [['colorby', 'mode', 'colorBy'], ['viewctl', 'view', 'view'], ['widthmode', 'w', 'widthMode']]) {
  $(id).addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b || !state) return
    state[key] = b.dataset[attr]; renderControls(); renderMap()
  })
}

// Color of a chunk under the current "color by" mode.
function chunkColor(m, name) {
  if (state.colorBy === 'codec') return CODEC_COLOR[m.codec] || '#94a3b8'
  if (state.colorBy === 'encoding') return ENC_COLOR[primaryEncoding(m)] || '#94a3b8'
  return state.colColor[name]
}

function renderMap() {
  const logical = state.view === 'logical'
  $('logical').hidden = !logical; $('bytes').hidden = logical; $('widthctl').hidden = !logical
  $('structsub').textContent = logical
    ? 'row groups stacked, columns across, pages inside each chunk — click a chunk to inspect it'
    : 'every byte in file order, to scale'
  if (logical) renderLogical(); else renderBytes()
  renderLegend()
}

// ── logical view ──
const chunkPages = (ri, ci) => {
  const cached = state.pageCache[ri + ':' + ci]
  if (!cached) return null
  return cached.filter(p => p.h).map((p, i) => {
    const h = p.h, d = h.data_page_header || h.data_page_header_v2 || h.dictionary_page_header || {}
    const dict = h.type === 'DICTIONARY_PAGE'
    return { kind: dict ? 'dict' : 'data', weight: dict ? 0 : num(d.num_values ?? 1), title: `Page ${i}: ${h.type.replace(/_/g, ' ').toLowerCase()} · ${bytes(h.compressed_page_size)} · ${fmtInt(d.num_values ?? 0)} values` }
  })
}
function cellSpec(ri, ci) {
  const m = state.meta.row_groups[ri].columns[ci].meta_data
  const sel = state.selected
  return { color: chunkColor(m, chunkName(m)), pages: chunkPages(ri, ci), selected: !!sel && sel.rg === ri && sel.col === ci }
}
function renderLogical() {
  const { meta } = state, rgs = meta.row_groups
  const first = rgs[0].columns.map(c => c.meta_data)
  const ncol = first.length
  let widths = first.map(() => 1)
  if (state.widthMode === 'size') {
    const tot = first.map((_, ci) => rgs.reduce((a, rg) => a + num(rg.columns[ci].meta_data.total_compressed_size), 0))
    const sum = tot.reduce((a, b) => a + b, 0) || 1
    widths = tot.map(t => Math.max(0.25, t / sum * ncol))
  }
  const maxRows = Math.max(1, ...rgs.map(r => num(r.num_rows)))
  const sizeOf = k => state.segs.filter(x => x.kind === k).reduce((a, x) => a + x.len, 0)
  const idxBytes = sizeOf('colidx') + sizeOf('offidx'), bloomBytes = sizeOf('bloom')
  const after = []
  const extra = [idxBytes && `column & offset indexes ${bytes(idxBytes)}`, bloomBytes && `bloom filters ${bytes(bloomBytes)}`].filter(Boolean)
  if (extra.length) after.push({ label: 'Optional', text: extra.join(' · '), cls: 'index' })
  after.push({ label: 'Footer', text: `Schema, row-group and column metadata, statistics · ${bytes(state.footerLen)}` })
  renderGrid($('logical'), {
    columns: first.map(m => ({ name: chunkName(m) })), widths, rotate: ncol > 14,
    before: { label: '', text: 'PAR1', cls: 'magic' },
    bands: rgs.map((rg, ri) => ({
      label: `Row group ${ri}`, sub: `${fmtInt(rg.num_rows)} rows`,
      sub2: bytes(rg.columns.reduce((a, c) => a + num(c.meta_data.total_compressed_size), 0)),
      height: Math.round(Math.min(150, Math.max(60, num(rg.num_rows) / maxRows * 150))),
      cells: rg.columns.map((_, ci) => cellSpec(ri, ci)),
    })),
    after,
  })
  $('logical').querySelector('.lg').classList.toggle('has-sel', !!state.selected)
}

// Page headers for every chunk are read in the background so the grid can show pages inside cells.
// (Very large files: first row group only, to keep this cheap.)
async function loadGridPages() {
  const my = state
  let jobs = []
  my.meta.row_groups.forEach((rg, ri) => rg.columns.forEach((_, ci) => jobs.push([ri, ci])))
  const capped = jobs.length > 900
  if (capped) jobs = jobs.filter(([ri]) => ri === 0)
  let next = 0, done = 0
  const worker = async () => {
    while (next < jobs.length) {
      const [ri, ci] = jobs[next++]
      try { await readPages(ri, ci) } catch { /* leave the cell solid */ }
      if (state !== my) return
      done++
      if (state.view === 'logical') updateCell($('logical'), ri, ci, cellSpec(ri, ci))
    }
  }
  await Promise.all(Array.from({ length: 6 }, worker))
  if (state === my && capped) $('structsub').textContent += ' · pages shown for row group 0 only (large file)'
}

$('logical').addEventListener('mousemove', e => {
  const el = e.target.closest('.lg-cell:not(.static)'); if (!el) return hideTip()
  const ri = +el.dataset.r, ci = +el.dataset.c
  const c = state.meta.row_groups[ri].columns[ci], m = c.meta_data
  const pages = state.pageCache[ri + ':' + ci]
  showTipAt(e, `Row group ${ri} · ${chunkName(m)}\n${m.type} · ${m.codec} · ${primaryEncoding(m)}\n${fmtInt(m.num_values)} values · ${bytes(m.total_compressed_size)} on disk${pages ? ` · ${pages.length} page${pages.length === 1 ? '' : 's'}` : ''}`)
})
$('logical').addEventListener('mouseleave', () => hideTip())
$('logical').addEventListener('click', e => {
  const el = e.target.closest('.lg-cell:not(.static)'); if (!el) return
  selectChunk(+el.dataset.r, +el.dataset.c, { scroll: true })
})

// ── byte-layout view ──
function renderBytes() {
  const { segs, file } = state
  const map = $('map'), mapW = map.clientWidth
  map.classList.toggle('has-sel', !!state.selected)
  map.innerHTML = segs.map((s, i) => {
    const small = s.kind === 'magic' || s.kind === 'footerlen'
    const grow = small ? Math.max(s.len, file.size * 0.004) : s.len
    const text = s.kind === 'chunk' ? s.name : s.kind === 'footer' ? 'footer' : ''
    const label = text && s.len / file.size * mapW > text.length * 6.6 + 14 ? text : '' // only when the whole name fits
    const sel = state.selected && s.kind === 'chunk' && s.rg === state.selected.rg && s.col === state.selected.col
    const clickable = s.kind === 'chunk' || s.kind === 'footer'
    return `<div class="seg${sel ? ' sel' : ''}${clickable ? '' : ' fixed'}" data-i="${i}" style="flex:${grow} 0 0;background:${segColor(s)}">${esc(label)}</div>`
  }).join('')

  // row-group strip: each row group spans from its first to last chunk byte
  $('rgstrip').innerHTML = state.meta.row_groups.map((rg, ri) => {
    const mine = segs.filter(x => x.kind === 'chunk' && x.rg === ri)
    if (!mine.length) return ''
    const a = Math.min(...mine.map(x => x.start)), b = Math.max(...mine.map(x => x.start + x.len))
    return `<div style="left:${a / file.size * 100}%;width:${(b - a) / file.size * 100}%">Row group ${ri} · ${fmtInt(rg.num_rows)} rows</div>`
  }).join('')
}

// ── legend (shared) ──
function renderLegend() {
  const chunks = state.meta.row_groups.flatMap(rg => rg.columns.map(c => c.meta_data))
  const sw = (c, n, cls = '') => `<span><i class="${cls}"${cls ? '' : ` style="background:${c}"`}></i>${esc(n)}</span>`
  let first
  if (state.colorBy === 'codec') first = `<span class="t">Codec</span>` + [...new Set(chunks.map(m => m.codec))].map(c => sw(CODEC_COLOR[c] || '#94a3b8', c)).join('')
  else if (state.colorBy === 'encoding') first = `<span class="t">Main encoding</span>` + [...new Set(chunks.map(primaryEncoding))].map(e => sw(ENC_COLOR[e] || '#94a3b8', e)).join('')
  else first = `<span class="t">Column</span><span>One hue per column, in schema order — hover for its name</span>`
  const kinds = new Set(state.segs.map(s => s.kind))
  const rest = state.view === 'logical' ? [] : [sw(STRUCT_COLOR.footer, 'footer'), kinds.has('colidx') && sw(STRUCT_COLOR.colidx, 'page indexes'), kinds.has('bloom') && sw(STRUCT_COLOR.bloom, 'bloom filters'), sw(STRUCT_COLOR.magic, 'magic bytes'), kinds.has('gap') && sw(STRUCT_COLOR.gap, 'unaccounted')].filter(Boolean)
  const pages = state.view === 'logical' ? `<div class="grp"><span class="t">Inside a chunk</span>${sw('#f59e0b', 'dictionary page', 'sw-dict')}<span>stacked data pages, height ∝ values</span></div>` : ''
  $('legend').innerHTML = `<div class="grp">${first}</div>${pages}${rest.length ? `<div class="grp"><span class="t">Structure</span>${rest.join('')}</div>` : ''}`
}

$('map').addEventListener('mousemove', e => {
  const el = e.target.closest('.seg'); if (!el) return hideTip()
  const s = state.segs[el.dataset.i]
  let t = `${s.label}\noffset ${fmtInt(s.start)} · ${bytes(s.len)} (${fmtInt(s.len)} B)\n${(s.len / state.file.size * 100).toFixed(2)}% of file`
  if (s.kind === 'chunk') {
    const m = state.meta.row_groups[s.rg].columns[s.col].meta_data
    t += `\n${m.type} · ${m.codec} · ${primaryEncoding(m)} · ${fmtInt(m.num_values)} values`
  }
  showTipAt(e, t)
})
function showTipAt(e, text) {
  const tip = $('tip')
  tip.textContent = text; tip.style.display = 'block'
  tip.style.left = Math.min(e.clientX + 14, innerWidth - 350) + 'px'; tip.style.top = (e.clientY + 16) + 'px'
}
const hideTip = () => { $('tip').style.display = 'none' }
$('map').addEventListener('mouseleave', hideTip)
$('map').addEventListener('click', e => {
  const el = e.target.closest('.seg'); if (!el) return
  const s = state.segs[el.dataset.i]
  if (s.kind === 'chunk') selectChunk(s.rg, s.col, { scroll: true })
  else if (s.kind === 'footer') setTab('footer', true)
})

// ───────────── tabs ─────────────
function renderTabs() {
  $('tabs').innerHTML = TABS.filter(([k]) => k !== 'guide' || state.guide).map(([k, l]) => {
    const chip = k === 'chunk' && state.selected ? `<span class="chip">${esc(chunkName(state.meta.row_groups[state.selected.rg].columns[state.selected.col].meta_data))}</span>` : ''
    return `<button class="tab" role="tab" data-k="${k}" aria-selected="${state.tab === k}">${l}${chip}</button>`
  }).join('')
}
$('tabs').addEventListener('click', e => { const t = e.target.closest('.tab'); if (t) setTab(t.dataset.k) })
function setTab(k, scroll) {
  state.tab = k; renderTabs(); renderPane()
  if (scroll) $('tabs').scrollIntoView({ behavior: 'smooth', block: 'start' })
}
function selectChunk(rg, col, { scroll = false } = {}) {
  state.selected = { rg, col }; state.tab = 'chunk'
  renderMap(); renderTabs(); renderPane()
  if (scroll) $('tabs').scrollIntoView({ behavior: 'smooth', block: 'start' })
}

async function renderPane() {
  const pane = $('pane'), t = state.tab
  if (t === 'guide') guidePane(pane)
  else if (t === 'schema') pane.innerHTML = schemaHtml()
  else if (t === 'rowgroups') pane.innerHTML = rowGroupsHtml()
  else if (t === 'chunk') await chunkPane(pane)
  else if (t === 'data') await dataPane(pane)
  else if (t === 'kv') kvPane(pane)
  else if (t === 'footer') { pane.replaceChildren(); mountJson(pane, state.meta, { openDepth: 2 }) }
}

// One delegated handler for everything clickable inside the pane.
$('pane').addEventListener('click', e => {
  const rgBtn = e.target.closest('[data-pick-rg]')
  if (rgBtn) { state.rg = +rgBtn.dataset.pickRg; renderPane(); return }
  const nav = e.target.closest('[data-nav-rg]')
  if (nav) { selectChunk(+nav.dataset.navRg, state.selected.col); return }
  const step = e.target.closest('[data-nav-col]')
  if (step) { selectChunk(state.selected.rg, state.selected.col + +step.dataset.navCol); return }
  if (e.target.closest('#pickFirst')) { selectChunk(0, 0); return }
  const tr = e.target.closest('tr[data-col]')
  if (tr) selectChunk(+(tr.dataset.rg ?? state.rg), +tr.dataset.col, { scroll: true })
})

// ───────────── example guide ─────────────
// Files written by scripts/make_example.py embed per-column explanations in their key-value metadata.
function readGuide(meta) {
  const kv = meta.key_value_metadata || []
  const g = kv.find(x => x.key === 'parquet_viewer_guide')
  if (!g) return null
  try { return { cols: JSON.parse(g.value), overview: kv.find(x => x.key === 'parquet_viewer_overview')?.value } } catch { return null }
}
function schemaPaths(meta) { // dotted path -> SchemaElement
  const out = {}; let i = 0
  const walk = prefix => {
    const el = meta.schema[i++]
    const p = prefix ? [...prefix, el.name] : []
    out[p.join('.')] = el
    for (let c = 0; c < (el.num_children || 0); c++) walk(p)
  }
  walk(null)
  return out
}
function guidePane(pane) {
  const { meta, guide } = state
  const rows = Object.entries(guide.cols).map(([name, note]) => {
    const leaves = []
    meta.row_groups.forEach((rg, ri) => rg.columns.forEach((c, ci) => { if (c.meta_data.path_in_schema[0] === name) leaves.push([ri, ci, c]) }))
    const uniq = f => [...new Set(leaves.map(([, , c]) => f(c.meta_data)))]
    const first = leaves.find(([ri]) => ri === 0) || leaves[0]
    const extras = [leaves.some(([, , c]) => c.meta_data.bloom_filter_offset != null) && 'bloom filter', leaves.some(([, , c]) => c.column_index_offset == null) && 'no page index'].filter(Boolean)
    const nLeaves = leaves.length / meta.row_groups.length
    return `<tr class="click" data-rg="${first[0]}" data-col="${first[1]}">
      <td class="name"><span class="sw" style="background:${state.colColor[chunkName(first[2].meta_data)]}"></span>${esc(name)}${nLeaves > 1 ? `<span class="dim" style="font-weight:400"> · ${nLeaves} leaves</span>` : ''}</td>
      <td>${uniq(m => m.type).map(esc).join('<br>')}</td>
      <td>${uniq(m => m.codec).map(codecPill).join('')}</td>
      <td class="enc">${[...new Set(leaves.flatMap(([, , c]) => c.meta_data.encodings))].map(encPill).join('')}${extras.map(x => `<span class="pill ghost">${x}</span>`).join('')}</td>
      <td class="wrap">${esc(note)}</td></tr>`
  }).join('')
  pane.innerHTML = `<p class="note">${guide.overview ? esc(guide.overview) + ' ' : ''}Types, codecs and encodings are read live from the footer; click a row to inspect that column's pages.</p>
    <table><thead><tr><th>Column</th><th>Physical type</th><th>Codec</th><th>Encodings</th><th>What it demonstrates</th></tr></thead><tbody>${rows}</tbody></table>`
}

// ───────────── schema ─────────────
function schemaHtml() {
  const schema = state.meta.schema
  let i = 0
  const rows = []
  const walk = depth => {
    const el = schema[i++]
    const group = !!el.num_children
    const logical = el.converted_type || (el.logical_type && el.logical_type.type) || ''
    rows.push(`<tr><td class="tree-name" style="padding-left:${12 + depth * 20}px"><span class="ico">${group ? '▾' : '•'}</span>${esc(el.name)}</td>
      <td>${group ? '<span class="dim">group</span>' : esc(el.type || '') + (el.type_length ? `(${el.type_length})` : '')}</td>
      <td>${el.repetition_type ? `<span class="pill ghost">${esc(el.repetition_type.toLowerCase())}</span>` : ''}</td>
      <td>${logical ? `<span class="pill">${esc(logical)}</span>` : ''}</td>
      <td class="dim">${group ? `${el.num_children} ${el.num_children === 1 ? 'child' : 'children'}` : ''}</td></tr>`)
    for (let c = 0; c < (el.num_children || 0); c++) walk(depth + 1)
  }
  walk(0)
  return `<p class="note">The schema is a tree stored flat in the footer. Only <b>leaf</b> nodes (•) get column chunks. <b>optional</b> and <b>repeated</b> fields need definition and repetition levels to encode nulls and nesting.</p>
    <table class="dense"><thead><tr><th>Field</th><th>Physical type</th><th>Repetition</th><th>Logical type</th><th></th></tr></thead><tbody>${rows.join('')}</tbody></table>`
}

// ───────────── row groups ─────────────
function rowGroupsHtml() {
  const { meta } = state
  const rg = meta.row_groups[state.rg]
  const picks = meta.row_groups.map((r, i) => `<button class="${i === state.rg ? 'on' : ''}" data-pick-rg="${i}">RG ${i}</button>`).join('')
  const maxC = Math.max(1, ...rg.columns.map(c => num(c.meta_data.total_compressed_size)))
  const body = rg.columns.map((c, ci) => {
    const m = c.meta_data, name = chunkName(m)
    const comp = num(m.total_compressed_size), unc = num(m.total_uncompressed_size)
    const st = m.statistics, mn = st?.min_value ?? st?.min, mx = st?.max_value ?? st?.max
    const extras = [m.bloom_filter_offset != null && 'bloom filter'].filter(Boolean)
    const cell = v => v === undefined ? '<span class="dim">—</span>' : fmtVal(v)
    return `<tr class="click" data-col="${ci}">
      <td class="name"><span class="sw" style="background:${state.colColor[name]}"></span>${esc(name)}<div class="subl">${esc(m.type)}</div></td>
      <td>${codecPill(m.codec)}</td>
      <td class="enc">${m.encodings.map(encPill).join('')}${extras.map(x => `<span class="pill ghost">${x}</span>`).join('')}</td>
      <td class="num" title="${bytes(unc)} uncompressed · ${fmtInt(m.num_values)} values">${bytes(comp)} <span class="dim">· ${comp ? (unc / comp).toFixed(2) + '×' : '—'}</span><div class="bar"><div style="width:${comp / maxC * 100}%"></div></div></td>
      <td class="cell range" title="${mn === undefined ? '' : esc(plain(mn) + ' … ' + plain(mx))}">${mn === undefined && mx === undefined ? '<span class="dim" style="font-family:system-ui">no statistics</span>' : `${cell(mn)} <span class="dim">…</span> ${cell(mx)}`}</td>
      <td class="num">${st?.null_count == null ? '<span class="dim">—</span>' : fmtInt(st.null_count)}</td></tr>`
  }).join('')
  const sorted = (rg.sorting_columns || []).map(sc => `<b>${esc(rg.columns[sc.column_idx]?.meta_data.path_in_schema.join('.') ?? sc.column_idx)}</b> ${sc.descending ? '↓' : '↑'}${sc.nulls_first ? ' (nulls first)' : ''}`).join(', ')
  return `<div class="rg-pick"><div class="segctl" role="group" aria-label="Row group">${picks}</div>
      <span class="meta">${fmtInt(rg.num_rows)} rows · ${bytes(num(rg.total_byte_size))} uncompressed${sorted ? ` · sorted by ${sorted}` : ''}</span></div>
    <p class="note">A <b>row group</b> is a horizontal slice of rows. Within it, each column is stored contiguously as a <b>column chunk</b>. Click a row to inspect its pages.</p>
    <table class="sticky1 tight"><thead><tr><th>Column</th><th>Codec</th><th class="enc">Encodings</th><th class="num">Compressed</th><th>Min … Max</th><th class="num">Nulls</th></tr></thead><tbody>${body}</tbody></table>`
}

// ───────────── pages in a chunk ─────────────
// A chunk is a back-to-back sequence of [Thrift PageHeader][page bytes]. We walk it by
// parsing each header and skipping compressed_page_size bytes to the next one.
async function readPages(rgI, colI) {
  const st = state // stay bound to the file this read was started for, even if another file is opened meanwhile
  const key = rgI + ':' + colI
  if (st.pageCache[key]) return st.pageCache[key]
  const m = st.meta.row_groups[rgI].columns[colI].meta_data
  const start = chunkStart(m), end = start + num(m.total_compressed_size)
  const pages = []
  let off = start
  while (off < end && pages.length < 5000) {
    const ab = await st.buf.slice(off, Math.min(off + 16384, st.file.size))
    const reader = { view: new DataView(ab), offset: 0 }
    let h
    try { h = parquetHeader(reader) } catch (e) { pages.push({ error: e.message, offset: off }); break }
    const hdrLen = reader.offset
    pages.push({ h, offset: off, hdrLen, total: hdrLen + h.compressed_page_size })
    off += hdrLen + h.compressed_page_size
  }
  return st.pageCache[key] = pages
}

async function chunkPane(pane) {
  if (!state.selected) {
    pane.innerHTML = `<div class="empty-state"><b>No column chunk selected</b>Click a segment in the file map, or a row in the Row groups tab, to see its pages.<br><button class="btn" id="pickFirst">Inspect the first column</button></div>`
    return
  }
  const { rg, col } = state.selected
  const rgs = state.meta.row_groups
  const c = rgs[rg].columns[col], m = c.meta_data
  const name = chunkName(m)
  pane.innerHTML = '<p class="note">Reading page headers…</p>'
  const pages = await readPages(rg, col)
  if (!state || state.tab !== 'chunk' || state.selected?.rg !== rg || state.selected?.col !== col) return

  const total = num(m.total_compressed_size), unc = num(m.total_uncompressed_size), colr = state.colColor[name]
  const pageColor = p => !p.h ? '#dc2626' : p.h.type === 'DICTIONARY_PAGE' ? '#f59e0b' : p.h.type === 'INDEX_PAGE' ? '#94a3b8' : colr
  const hasDictPage = pages.some(p => p.h?.type === 'DICTIONARY_PAGE')
  const rows = pages.map((p, i) => {
    if (!p.h) return `<tr><td>${i}</td><td colspan="8" style="color:var(--danger)">${esc(p.error)}</td></tr>`
    const h = p.h, d = h.data_page_header || h.data_page_header_v2 || h.dictionary_page_header || {}
    const st = d.statistics
    const details = h.data_page_header_v2
      ? `rows ${fmtInt(d.num_rows)} · nulls ${fmtInt(d.num_nulls)} · def ${d.definition_levels_byte_length} B · rep ${d.repetition_levels_byte_length} B`
      : h.data_page_header ? `def ${esc(d.definition_level_encoding)} · rep ${esc(d.repetition_level_encoding)}` : ''
    const crc = h.crc !== undefined ? `<span class="dim">crc ${(h.crc >>> 0).toString(16).padStart(8, '0')}</span>` : ''
    const minmax = st && (st.min_value ?? st.min) !== undefined ? `${fmtVal(st.min_value ?? st.min)} … ${fmtVal(st.max_value ?? st.max)}` : ''
    return `<tr><td class="dim">${i}</td><td><span class="pill" style="background:${pageColor(p)};color:#fff;box-shadow:none">${esc(h.type.replace('_PAGE', '').replace('_V2', ' v2'))}</span></td>
      <td class="num mono">${fmtInt(p.offset)}</td><td class="num">${p.hdrLen} B</td>
      <td class="num">${bytes(h.compressed_page_size)}</td><td class="num">${bytes(h.uncompressed_page_size)}</td>
      <td class="num">${d.num_values != null ? fmtInt(d.num_values) : '—'}</td>
      <td>${d.encoding ? encPill(d.encoding) : ''}</td>
      <td class="mono">${[details, crc, minmax].filter(Boolean).join(' · ')}</td></tr>`
  }).join('')
  const bar = pages.map((p, i) => `<div title="Page ${i}: ${p.h ? p.h.type : 'error'}" style="flex:${p.total || 1} 0 0;background:${pageColor(p)}"></div>`).join('')
  const ps = (m.encoding_stats || []).map(s => `<span class="pill ghost">${esc(s.page_type.replace('_', ' ').toLowerCase())} · ${esc(s.encoding)} × ${s.count}</span>`).join('')
  const extras = [
    m.bloom_filter_offset != null && `bloom filter (${bytes(m.bloom_filter_length || 0)})`, c.column_index_offset != null && 'column index', c.offset_index_offset != null && 'offset index',
  ].filter(Boolean).map(x => `<span class="pill ghost">${x}</span>`)
  if ((m.statistics?.min_value ?? m.statistics?.min) === undefined) extras.push('<span class="pill warn">no min/max statistics</span>')
  const defHist = m.size_statistics?.definition_level_histogram

  const colBtn = (delta, label) => `<button class="btn" data-nav-col="${delta}" ${rgs[rg].columns[col + delta] ? '' : 'disabled'}>${label}</button>`
  pane.innerHTML = `
    <div class="chunknav">
      <div class="segctl" role="group" aria-label="Row group">${rgs.map((_, i) => `<button class="${i === rg ? 'on' : ''}" data-nav-rg="${i}">RG ${i}</button>`).join('')}</div>
      <span class="spacer"></span>${colBtn(-1, '← Previous column')}${colBtn(1, 'Next column →')}
    </div>
    <div class="chunkhead"><h3><span class="sw" style="display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:8px;background:${colr}"></span>${esc(name)}</h3>
      <div class="sub">Row group ${rg} · ${esc(m.type)} · ${codecPill(m.codec)}</div></div>
    <div class="facts">
      <div><div class="k">On disk</div><div class="v">${bytes(total)}</div></div>
      <div><div class="k">Uncompressed</div><div class="v">${bytes(unc)} <span class="dim" style="font-weight:400">(${total ? (unc / total).toFixed(2) : '—'}×)</span></div></div>
      <div><div class="k">Values</div><div class="v">${fmtInt(m.num_values)}</div></div>
      <div><div class="k">Pages</div><div class="v">${pages.length}</div></div>
      <div><div class="k">Byte range</div><div class="v mono">${fmtInt(chunkStart(m))} → ${fmtInt(chunkStart(m) + total)}</div></div>
    </div>
    <div class="kvrows">
      <span class="k">Encodings</span><span>${m.encodings.map(encPill).join('')}</span>
      ${ps ? `<span class="k">Page types</span><span>${ps}</span>` : ''}
      <span class="k">Extras</span><span>${extras.join('') || '<span class="dim">none</span>'}</span>
      ${defHist?.length ? `<span class="k">Definition levels</span><span class="mono">${defHist.map((n, i) => `level ${i}: ${fmtInt(n)}`).join(' · ')} <span class="dim">— highest level = value present; lower = null at some ancestor</span></span>` : ''}
      ${c.file_path ? `<span class="k">file_path</span><span class="mono">${esc(c.file_path)}</span>` : ''}
    </div>
    <h3 class="subhead">Pages</h3>
    <p class="note">Each page is a Thrift <b>PageHeader</b> followed by (compressed) data: repetition levels, definition levels, then values.${hasDictPage ? ' The amber <b>dictionary page</b> comes first and holds the distinct values that data pages index into.' : ''}</p>
    <div class="pagebar" title="Pages drawn to scale">${bar}</div>
    <table><thead><tr><th>#</th><th>Type</th><th class="num">Offset</th><th class="num">Header</th><th class="num">Compressed</th><th class="num">Uncompressed</th><th class="num">Values</th><th>Encoding</th><th>Details</th></tr></thead><tbody>${rows}</tbody></table>
    <div id="pageidx"></div>`
  pageIndexInto($('pageidx'), c, name)
}

// Page index = ColumnIndex (per-page min/max/null) + OffsetIndex (where each page is, which row it starts at),
// stored OUTSIDE the chunk so readers can skip pages without touching the data.
async function pageIndexInto(host, c, name) {
  const head = badge => `<h3 class="subhead">Page index ${badge || ''}</h3>`
  if (c.column_index_offset == null && c.offset_index_offset == null) { host.innerHTML = head() + '<p class="note">This chunk has no column or offset index.</p>'; return }
  try {
    const sel = state.selected
    const read = async (off, len) => ({ view: new DataView(await state.buf.slice(num(off), num(off) + len)), offset: 0 })
    const el = state.schemaByPath[name]
    const ci = c.column_index_offset != null ? readColumnIndex(await read(c.column_index_offset, c.column_index_length), el) : null
    const oi = c.offset_index_offset != null ? readOffsetIndex(await read(c.offset_index_offset, c.offset_index_length)) : null
    if (!state || state.selected !== sel) return
    const n = Math.max(ci?.min_values.length ?? 0, oi?.page_locations.length ?? 0)
    const dash = '<span class="dim">—</span>'
    const rows = Array.from({ length: Math.min(n, 500) }, (_, i) => {
      const loc = oi?.page_locations[i]
      const mm = arr => ci ? (ci.null_pages[i] ? '<span class="null">all null</span>' : fmtVal(arr[i])) : dash
      return `<tr><td class="dim">${i}</td><td class="num">${loc ? fmtInt(loc.first_row_index) : dash}</td><td class="num mono">${loc ? fmtInt(loc.offset) : dash}</td><td class="num">${loc ? bytes(loc.compressed_page_size) : dash}</td>
        <td class="cell clip mono">${mm(ci?.min_values)}</td><td class="cell clip mono">${mm(ci?.max_values)}</td><td class="num">${ci?.null_counts ? fmtInt(ci.null_counts[i]) : dash}</td></tr>`
    }).join('')
    const order = ci ? (ci.boundary_order === 'UNORDERED'
      ? 'Boundary order <b>UNORDERED</b>: page ranges overlap, so a lookup may need to check every page whose range could match.'
      : `Boundary order <b>${esc(ci.boundary_order)}</b>: page min/max are sorted, so readers can binary-search straight to the right page.`) : ''
    host.innerHTML = head(ci ? `<span class="pill ghost">${esc(ci.boundary_order.toLowerCase())}</span>` : '') +
      `<p class="note">Per-page min/max and row ranges, read from the ColumnIndex and OffsetIndex structures stored outside the chunk (see the file map). A query like <code>id = 5000</code> can use them to read only the page that could contain it. ${order}</p>
      <table><thead><tr><th>Page</th><th class="num">First row</th><th class="num">Offset</th><th class="num">Size</th><th>Min</th><th>Max</th><th class="num">Nulls</th></tr></thead><tbody>${rows}</tbody></table>${n > 500 ? '<p class="note">First 500 pages shown.</p>' : ''}`
  } catch (e) { host.innerHTML = head() + `<p class="note" style="color:var(--danger)">Could not read page index: ${esc(e.message)}</p>` }
}

// ───────────── data preview ─────────────
const PAGE = 50
async function dataPane(pane) {
  const total = Number(state.meta.num_rows)
  state.dataStart = Math.min(state.dataStart, Math.max(0, total - 1))
  const start = state.dataStart, end = Math.min(start + PAGE, total)
  pane.innerHTML = '<p class="note">Decoding rows…</p>'
  let rows
  try {
    rows = await parquetReadObjects({ file: state.buf, metadata: state.meta, compressors, utf8: false, rowStart: start, rowEnd: end })
  } catch (e) { pane.innerHTML = `<p class="note" style="color:var(--danger)">Could not decode rows: ${esc(e.message)}</p>`; return }
  if (!state || state.tab !== 'data') return
  const cols = rows.length ? Object.keys(rows[0]) : []
  const isNum = c => { const v = rows.map(r => r[c]).find(x => x !== null && x !== undefined); return typeof v === 'number' || typeof v === 'bigint' }
  const typeOf = c => { const el = state.schemaByPath[c]; return !el ? '' : el.num_children ? (el.converted_type || 'group') : (el.converted_type || el.logical_type?.type || el.type || '').toLowerCase() }
  const numeric = Object.fromEntries(cols.map(c => [c, isNum(c)]))
  pane.innerHTML = `<div class="pager">
      <button class="btn" id="prev" ${start === 0 ? 'disabled' : ''}>← Previous</button>
      <span class="range">Rows <b>${fmtInt(start)}–${fmtInt(end - 1)}</b> of ${fmtInt(total)}</span>
      <button class="btn" id="next" ${end >= total ? 'disabled' : ''}>Next →</button>
      <span class="spacer"></span><label class="dim">Jump to row <input class="input" id="jump" type="number" min="0" max="${total - 1}" value="${start}"></label></div>
    <table><thead><tr><th class="num">#</th>${cols.map(c => `<th class="${numeric[c] ? 'num' : ''}">${esc(c)}<span class="ty">${esc(typeOf(c))}</span></th>`).join('')}</tr></thead>
    <tbody>${rows.map((r, i) => `<tr><td class="num dim">${start + i}</td>${cols.map(c => `<td class="cell ${numeric[c] ? 'num' : ''}" title="${esc(plain(r[c]))}">${fmtVal(r[c])}</td>`).join('')}</tr>`).join('')}</tbody></table>`
  $('prev').onclick = () => { state.dataStart = Math.max(0, start - PAGE); renderPane() }
  $('next').onclick = () => { state.dataStart = start + PAGE; renderPane() }
  $('jump').onchange = e => { state.dataStart = Math.max(0, Math.floor(+e.target.value) || 0); renderPane() }
}

// ───────────── key-value metadata ─────────────
function kvPane(pane) {
  const kv = state.meta.key_value_metadata || []
  if (!kv.length) { pane.innerHTML = '<p class="note">This file has no file-level key-value metadata.</p>'; return }
  pane.innerHTML = '<p class="note">Free-form file metadata. Writers such as pandas and Arrow store their own schema here.</p>'
  for (const x of kv) {
    const block = document.createElement('div'); block.className = 'kvblock'
    const h = document.createElement('h3'); h.textContent = x.key
    const box = document.createElement('div')
    block.append(h, box); pane.append(block)
    let parsed
    try { parsed = JSON.parse(x.value || '') } catch {}
    if (parsed !== undefined && isCont(parsed)) mountJson(box, parsed, { openDepth: 1 })
    else { const pre = document.createElement('pre'); pre.textContent = x.value || ''; box.append(pre) }
  }
}

// ───────────── built-in examples & file input ─────────────
async function loadExample(name) {
  const r = await fetch('/samples/' + name)
  if (!r.ok) throw new Error(`Example ${name} not found on the server (HTTP ${r.status})`)
  await load(new File([await r.blob()], name))
}
const v2 = () => loadExample('example_v2.parquet').catch(showErr), v1 = () => loadExample('example_v1.parquet').catch(showErr)
const pick = $('picker')
const activate = (id, fn) => { $(id).onclick = fn; $(id).onkeydown = e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fn() } } }
activate('optv2', v2); activate('optv1', v1); activate('ex2', v2); activate('ex1', v1); activate('upload', () => pick.click())
$('open').onclick = () => pick.click()
pick.onchange = () => { if (pick.files[0]) { load(pick.files[0]); pick.value = '' } }
for (const ev of ['dragover', 'dragenter']) addEventListener(ev, e => { e.preventDefault(); $('upload').classList.add('over') })
for (const ev of ['dragleave', 'drop']) addEventListener(ev, e => { e.preventDefault(); $('upload').classList.remove('over') })
addEventListener('drop', e => { const f = e.dataTransfer.files[0]; if (f) load(f) })

// ───────────── landing diagram ─────────────
// The same grid used for real files, drawn for a tiny made-up table.
function landingDiagram() {
  const names = ['id', 'name', 'price', 'tags', 'ts']
  const data = (...w) => w.map(weight => ({ kind: 'data', weight }))
  const dict = { kind: 'dict', title: 'dictionary page' }
  const cell = (i, pages) => ({ color: columnColor(i, names.length), pages, static: true })
  renderGrid($('anatomy-diagram'), {
    columns: names.map(name => ({ name })), widths: names.map(() => 1), rotate: false,
    before: { label: '', text: 'PAR1', cls: 'magic' },
    bands: [
      { label: 'Row group 0', sub: 'first N rows', height: 112, cells: [cell(0, data(3, 3, 2)), cell(1, [dict, ...data(4, 4)]), cell(2, data(4, 4)), cell(3, [dict, ...data(3, 3, 2)]), cell(4, data(8))] },
      { label: 'Row group 1', sub: 'next N rows', height: 84, cells: [cell(0, data(4, 4)), cell(1, [dict, ...data(5)]), cell(2, data(5, 3)), cell(3, [dict, ...data(4, 4)]), cell(4, data(8))] },
    ],
    after: [{ label: 'Footer', text: 'Schema · where every column chunk lives · sizes · min/max statistics' }, { label: '', text: 'footer length · PAR1', cls: 'magic' }],
  })
}
landingDiagram()

addEventListener('resize', () => { if (state && state.view === 'logical' && !$('app').hidden) fitHeads($('logical')) })

// ───────────── theme ─────────────
// "auto" follows the system; "light"/"dark" override it. Saved in localStorage (applied early by a tiny script in index.html).
function renderTheme() {
  const cur = document.documentElement.dataset.theme || 'auto'
  $('themectl').querySelectorAll('button').forEach(b => { const on = b.dataset.theme === cur; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on) })
}
$('themectl').addEventListener('click', e => {
  const b = e.target.closest('button'); if (!b) return
  const t = b.dataset.theme
  if (t === 'auto') delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t
  try { t === 'auto' ? localStorage.removeItem('theme') : localStorage.setItem('theme', t) } catch { /* private mode: still works for this session */ }
  renderTheme()
})
renderTheme()
