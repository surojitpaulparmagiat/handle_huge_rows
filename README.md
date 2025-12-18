# handle_huge_rows

This project parses a large CSV (up to 1M+ rows), writes valid rows to batch CSV files under `temp/`, and can bulk-load them into MySQL.

## Quick start

```powershell
cd C:\Users\surojit\WebstormProjects\handle_huge_rows
npm install
node index.js
```

The script will parse `One_million_rows.csv` from the project root, write `temp/CSV_*.csv` files, and print timing information.

To load the generated CSV files into MySQL, uncomment the call to `loadCSVFilesToMySQL` at the bottom of `index.js` and ensure `DB_CONFIG` points at a MySQL instance with an `aud_source_data` table matching the CSV columns.

