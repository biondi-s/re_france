Work in Progress

# re_france

Utilities for scraping and consolidating French DVF data.

## Setup
- Use Python 3.8+.
- Optional: create a virtual environment.
- Install deps:
```bash
pip install -r requirements.txt
```

## Scraping (Dynimmo demo)
Fetch list data from the Acordyn Dynimmo demo:
```bash
python scrape_dynimmo.py --max-pages 5 --page-size 50 --sleep 0.5 -o dynimmo_data.csv
```
Adjust pagination/sleep to control load on the remote service.

## Combining yearly CSVs
Concatenate the `data/*full.csv` files, add a `year` column from filenames, and stream to Parquet:
```bash
python concat_csvs_to_parquet.py
```
The output is `data/dvf_2020_2025.parquet` (Snappy compressed). Streaming keeps memory usage reasonable even for large CSVs.
