# Covid 19 and Trade Flow
Understand the impact of pandemic to the international trades

# Data Modeling
Collect data from multiple resources and join together
## Dimension Tables
- Countries
- Classification

## Facts Tables
- Trades (import and export)
- Covid-19 total cases by countries
   
# ETL Process

# Example use

## 1. Track covid cases of countries over time

```
SELECT co.country_name, ca.*
FROM cases ca
JOIN countries co
ON ca.country_id = co.country_id
WHERE co.country_name = 'China'
ORDER BY period;
```

| country_name | country_id | period     | confirmed | deaths | recovered | active |
|--------------|------------|------------|-----------|--------|-----------|--------|
| China        | 156        | 2020-01-31 | 9783      | 213    | 214       | 6291   |
| China        | 156        | 2020-02-29 | 79251     | 2835   | 39279     | 37137  |
| China        | 156        | 2020-03-31 | 82279     | 3309   | 76200     | 2770   |
| China        | 156        | 2020-04-30 | 83956     | 4637   | 78523     | 796    |



## 2. Track total trades of countries over time

```
SELECT c1.country_name as country_from, c2.country_name as country_to, trade_time, cl.classification_description,
       trade_value, trade_weight, t.import_export_code
FROM trades t
JOIN countries c1
ON t.country_from = c1.country_id
JOIN countries c2
     ON t.country_to = c2.country_id
JOIN classifications cl
     ON t.classification_code = cl.classification_id
WHERE c1.country_name = 'USA' AND import_export_code IN ('Exports', 'Imports')
ORDER BY trade_time, t.classification_code, import_export_code;
```

| country_from | country_to | trade_time | classification                                                      | trade_value   | trade_weight | import_export_code |
|--------------|------------|------------|---------------------------------------------------------------------|---------------|--------------|--------------------|
| USA          | World      | 2020-01-01 | 01 - Animals; live                                                  | 88499362.00   | 0.00         | Exports            |
| USA          | World      | 2020-01-01 | 01 - Animals; live                                                  | 274231883.00  | 0.00         | Imports            |
| USA          | World      | 2020-01-01 | 02 - Meat and edible meat offal                                     | 1634223239.00 | 0.00         | Exports            |
| USA          | World      | 2020-01-01 | 02 - Meat and edible meat offal                                     | 766891700.00  | 0.00         | Imports            |
| USA          | World      | 2020-01-01 | 03 - Fish and crustaceans, molluscs and other aquatic invertebrates | 189313853.00  | 0.00         | Exports            |
| USA          | World      | 2020-01-01 | 03 - Fish and crustaceans, molluscs and other aquatic invertebrates | 1611574458.00 | 0.00         | Imports            |


## 3. TODO: total cases vs total trades over time
