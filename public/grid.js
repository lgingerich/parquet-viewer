// The "logical" picture of a Parquet file: row groups stacked top to bottom, columns across,
// each cell a column chunk, each chunk a vertical stack of pages (dictionary page first).
// Used both for the real file and for the static diagram on the landing page.
import { esc } from './util.js'

/** A chunk's pages: dictionary page = fixed thin strip; data pages share the rest by value count. */
export const pagesHtml = pages => (pages?.length ? pages : [{ kind: 'data', weight: 1 }]).map(p =>
  p.kind === 'dict'
    ? `<i class="pg dict" title="${esc(p.title || 'dictionary page')}"></i>`
    : `<i class="pg" style="flex:${Math.max(p.weight, 0.001)} 1 0" title="${esc(p.title || 'data page')}"></i>`).join('')

const cellHtml = (cell, r, c, w) =>
  `<div class="lg-cell${cell.selected ? ' sel' : ''}${cell.static ? ' static' : ''}" data-r="${r}" data-c="${c}" style="--w:${w};--c:${cell.color}">${pagesHtml(cell.pages)}</div>`

/**
 * spec = {
 *   columns: [{ name }], widths: [number], rotate: boolean,
 *   bands:   [{ label, sub, sub2, height, cells: [{ color, pages, selected }] }],
 *   after:   [{ label, text, cls }]        // full-width strips below the bands (footer, indexes…)
 *   before:  { label, text, cls }          // strip above (header magic)
 * }
 */
export function renderGrid(host, spec) {
  const { columns, widths, rotate, bands, before, after = [], axis = true } = spec
  const strip = s => `<div class="lg-row"><div class="lg-gut">${esc(s.label || '')}</div><div class="lg-strip ${s.cls || ''}">${esc(s.text || '')}</div></div>`
  const heads = columns.map((c, i) => `<div class="lg-head${rotate ? ' rot' : ''}" style="--w:${widths[i]}" title="${esc(c.name)}"><span>${esc(c.name)}</span></div>`).join('')
  host.innerHTML = `<div class="lg" style="--n:${columns.length}">
    ${before ? strip(before) : ''}
    <div class="lg-row lg-headrow"><div class="lg-gut axis">${axis ? 'Row groups ↓<br>Columns →' : ''}</div><div class="lg-cells heads">${heads}</div></div>
    ${bands.map((b, r) => `<div class="lg-row lg-band" style="height:${b.height}px"><div class="lg-gut"><b>${esc(b.label)}</b><span>${esc(b.sub || '')}</span><span>${esc(b.sub2 || '')}</span></div>
      <div class="lg-cells">${b.cells.map((cell, c) => cellHtml(cell, r, c, widths[c])).join('')}</div></div>`).join('')}
    ${after.map(strip).join('')}
  </div>`
  if (rotate) fitHeads(host)
}

/** Tilted labels extend up and to the right; shorten the ones near the right edge so nothing overflows the page. */
export function fitHeads(host) {
  const lg = host.querySelector('.lg'); if (!lg) return
  const limit = Math.min(lg.getBoundingClientRect().right + 24, document.documentElement.clientWidth - 8)
  lg.querySelectorAll('.lg-head.rot').forEach(h => {
    const r = h.getBoundingClientRect()
    const room = (limit - (r.left + r.width / 2)) / Math.SQRT1_2 // horizontal room -> label length at 45°
    h.firstElementChild.style.maxWidth = Math.max(40, Math.min(130, room)) + 'px'
  })
}

/** Replace one cell's pages in place (page headers are loaded asynchronously after the first paint). */
export function updateCell(host, r, c, cell) {
  const el = host.querySelector(`.lg-cell[data-r="${r}"][data-c="${c}"]`)
  if (el) { el.innerHTML = pagesHtml(cell.pages); el.style.setProperty('--c', cell.color) }
}
