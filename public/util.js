export const $ = id => document.getElementById(id)
export const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]))
export const num = n => Number(n)
export const fmtInt = n => Number(n).toLocaleString()

export function bytes(n) {
  n = Number(n)
  if (n < 1024) return n + ' B'
  const u = ['KB', 'MB', 'GB', 'TB']; let i = -1
  do { n /= 1024; i++ } while (n >= 1024 && i < 3)
  return n.toFixed(n < 10 ? 2 : 1) + ' ' + u[i]
}

/** Any decoded Parquet value -> short display string. */
export function plain(v) {
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'bigint') return v.toString()
  if (v instanceof Date) return isNaN(v) ? 'Invalid Date' : v.toISOString()
  if (v instanceof Uint8Array) return '0x' + [...v.slice(0, 16)].map(b => b.toString(16).padStart(2, '0')).join('') + (v.length > 16 ? '…' : '')
  if (typeof v === 'object') return JSON.stringify(v, (_, x) => typeof x === 'bigint' ? x.toString() : x instanceof Uint8Array ? plain(x) : x)
  return String(v)
}
export const fmtVal = v => v === null || v === undefined ? '<span class="null">null</span>' : esc(plain(v))
