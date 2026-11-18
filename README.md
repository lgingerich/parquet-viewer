# Parquet Viewer

A small web app for seeing how a Parquet file is organized on the inside. Open a file and explore its row groups, column chunks, pages, encodings, compression, statistics, schema and data.

**Try it: [lgingerich.github.io/parquet-viewer](https://lgingerich.github.io/parquet-viewer/)**

Start from one of the built-in examples or drop in your own `.parquet` file. Everything runs in your browser: files you open are read locally and never uploaded anywhere. Light and dark themes are built in: use the switch in the top right, or leave it on auto to match your system.

![File structure: row groups stacked top to bottom, columns across, pages inside each column chunk](docs/screenshots/structure-light.png#gh-light-mode-only)
![File structure: row groups stacked top to bottom, columns across, pages inside each column chunk](docs/screenshots/structure-dark.png#gh-dark-mode-only)

## What you can do

### See how the file is laid out

Row groups are stacked top to bottom, columns run across, and each cell is a column chunk made of pages. The striped amber strip is a dictionary page. Click any chunk to inspect it, hover for details, and color the grid by column, codec or encoding.

### Look inside a column chunk

Every page in a chunk, with its type, size, encoding, value count and checksum, plus the page index (per-page min/max). This one shows dictionary encoding falling back to plain pages partway through a chunk.

![Pages in a column chunk](docs/screenshots/pages-light.png#gh-light-mode-only)
![Pages in a column chunk](docs/screenshots/pages-dark.png#gh-dark-mode-only)

### Or see the raw byte layout

Switch to the byte view to see every byte of the file in order, to scale.

![Byte layout, colored by codec](docs/screenshots/byte-layout-light.png#gh-light-mode-only)
![Byte layout, colored by codec](docs/screenshots/byte-layout-dark.png#gh-dark-mode-only)

### Learn from the built-in examples

Two synthetic example files use nearly every Parquet feature: five codecs, seven encodings, dictionary fallback, bloom filters, page indexes, nested types and more. An **Example guide** tab explains what each column demonstrates.

## Run it locally

You need [Node.js](https://nodejs.org) installed.

```
npm install
npm start
```

Then open http://localhost:8765.

Every push to `main` redeploys the hosted version through GitHub Actions (`.github/workflows/pages.yml`).
