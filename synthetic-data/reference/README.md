# Generator reference data

Public reference lists the synthetic population is drawn from. The generator job
loads these CSVs into `<catalog>.reference` (see `generate.py`) and every pick is a
stable hash into integer `[lo, hi)` ranges, so the same seed always yields the same
people. None of these files contain synthetic people, benchmark truth, or matching
rules. `build_reference.py` regenerates them byte-for-byte from the sources below.

| File | Built from | Source | Licence |
|---|---|---|---|
| `au_localities.csv` | Delivery-area localities (suburb, state, postcode, ABS SA4) | [matthewproctor/australianpostcodes](https://github.com/matthewproctor/australianpostcodes) `australian_postcodes.csv` (updated Sep 2025) | No licence file is published; the author describes it as a community-sourced collation of publicly available postal data. Used here as the project brief suggested, for a synthetic demo only. |
| `home_locality_weights.csv` | Home-location weights per heritage group over the gazetteer | Derived from the file above; SA4 shares and heritage concentrations are curated in `build_reference.py` | Same as above |
| `given_names.csv` (culture `anglo`) | Birth-decade name frequencies | [NSW Popular baby names 1952 to 2025](https://data.nsw.gov.au/data/dataset/popular-baby-names-from-1952), NSW Registry of Births, Deaths and Marriages | CC BY 4.0 |
| `given_names.csv`, `family_names.csv` (tails and other heritages) | Locale person-name lists: en_NZ, en_GB, en_IE, en_IN, zh_CN, zh_TW, ko_KR, el_GR, it_IT, ja_JP, de_DE, nl_NL, hr_HR, es_ES, pt_BR, fr_FR, id_ID, tr_TR | [Faker](https://github.com/joke2k/faker) 40.x person providers | MIT |
| Chinese romanisation | Hanzi to Hanyu Pinyin | [pypinyin](https://github.com/mozillazg/python-pinyin) | MIT |
| `family_names.csv` (culture `anglo`) | Surname frequencies, filtered to English/Celtic-origin names | [US Census Bureau 2010 surnames](https://www2.census.gov/topics/genealogy/2010surnames/names.zip) blended with the Faker en_NZ weighted list | US Government work, public domain |
| Curated heads (Vietnamese, Lebanese, Filipino, Korean, Cantonese, common Greek/Italian/Indian names), street names, overseas districts, email domains, nicknames, twin name pairs | Hand-curated common names and places | `build_reference.py` | Project-authored |
| `employers.csv` | 342 fictional employers with plausible `.com.au`/`.com` domains, heavy-tailed sizes, offices in real business districts | Generated in `build_reference.py` from word lists | Project-authored; names are invented and not intended to reference real organisations |

Korean given names are romanised with Revised Romanization and Greek names with a
simplified ELOT 743 transliteration, both implemented in `build_reference.py`.
Diacritics are folded to ASCII, which is how most Australian booking and POS
systems store names.

To rebuild (inputs are downloaded to a scratch folder, not committed):

```bash
uv run --with faker==40.39.0 --with pypinyin --with pandas \
  python synthetic-data/reference/build_reference.py \
  --postcodes /tmp/au_postcodes.csv \
  --nsw-names /tmp/popular_baby_names_1952_to_2025.csv \
  --census /tmp/Names_2010Census.csv
```
