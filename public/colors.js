// Color semantics shared by the file map, legends and pills.
export const ENC_COLOR = {
  PLAIN: '#94a3b8', RLE: '#14b8a6', RLE_DICTIONARY: '#f59e0b', PLAIN_DICTIONARY: '#f59e0b', BIT_PACKED: '#64748b',
  DELTA_BINARY_PACKED: '#3b82f6', DELTA_LENGTH_BYTE_ARRAY: '#6366f1', DELTA_BYTE_ARRAY: '#8b5cf6', BYTE_STREAM_SPLIT: '#ec4899',
}
export const CODEC_COLOR = {
  UNCOMPRESSED: '#94a3b8', SNAPPY: '#14b8a6', GZIP: '#f59e0b', BROTLI: '#f97316', ZSTD: '#3b82f6', LZ4: '#8b5cf6', LZ4_RAW: '#8b5cf6', LZO: '#ec4899',
}
export const STRUCT_COLOR = { magic: '#a1a1aa', footerlen: '#a1a1aa', footer: '#475569', colidx: '#78716c', offidx: '#78716c', bloom: '#ca8a04', gap: '#d6d3d1' }

/** Spread columns around the hue wheel; alternate lightness so neighbours stay distinguishable. */
export const columnColor = (i, n) => `hsl(${Math.round(i / Math.max(n, 1) * 320 + 8)} ${i % 2 ? 58 : 66}% ${i % 2 ? 56 : 46}%)`

/** The encoding that best characterises a chunk (what the data pages mostly use). */
export function primaryEncoding(m) {
  const e = new Set(m.encodings)
  if (e.has('RLE_DICTIONARY') || e.has('PLAIN_DICTIONARY')) return 'RLE_DICTIONARY'
  for (const k of ['DELTA_BINARY_PACKED', 'DELTA_BYTE_ARRAY', 'DELTA_LENGTH_BYTE_ARRAY', 'BYTE_STREAM_SPLIT']) if (e.has(k)) return k
  if (e.has('PLAIN')) return 'PLAIN'
  return e.has('RLE') ? 'RLE' : [...e][0] || 'PLAIN'
}

export const encPill = e => `<span class="pill"><i style="background:${ENC_COLOR[e] || '#94a3b8'}"></i>${e}</span>`
export const codecPill = c => `<span class="pill"><i style="background:${CODEC_COLOR[c] || '#94a3b8'}"></i>${c}</span>`
