# Raw Data — Download Instructions

The raw CSV files are not included in this repository due to file size (14.2GB combined).

## Dataset

**E-commerce Behaviour Data from Multi-Category Store**  
Author: Mikhail Kechinov  
Source: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

## Files Required

| Filename | Size | Rows | Rename to |
|---|---|---|---|
| 2019-Oct.csv | 8.7GB | 42,481,998 | events_oct.csv |
| 2019-Nov.csv | 5.5GB | 67,395,246 | events_nov.csv |

## Download Steps

1. Go to the Kaggle dataset link above
2. Sign in to your free Kaggle account
3. Click the **Download** button
4. Extract the zip file
5. Rename the files as shown in the table above
6. Place both files in this directory: `data/raw/`

## Important Warning

**Do NOT open these files in Excel.**

Excel has a hard row limit of 1,048,575 rows. Opening either file in Excel will silently truncate it from 40+ million rows to ~1 million rows with no warning. Use Notepad++ to inspect the files instead.

## After Downloading

Run the scripts in this order from the project root:

```
schema/01_create_tables.sql
schema/02_load_data.sql
```

Follow the CMD instructions inside `02_load_data.sql` for the \COPY commands.
Expected load time: 20-30 minutes for both files combined.
