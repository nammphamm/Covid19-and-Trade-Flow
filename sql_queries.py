# DROP TABLES
drop_table_queries = []
for table in ['countries', 'classifications', 'trades', 'import_export_codes','cases']:
    drop_table_queries.append("""
    DROP TABLE IF EXISTS {}""".format(table))

# CREATE TABLES
country_table_create = ("""
    CREATE TABLE IF NOT EXISTS countries
    (country_id text PRIMARY KEY,
    country_name text,
    region text,
    GDP_2017 FLOAT,
    GDP_per_Capita_2017 FLOAT,
    Population_2017 FLOAT,
    Military_Expenditure_2017 FLOAT)""")

classification_table_create = ("""
    CREATE TABLE IF NOT EXISTS classifications
    (classification_id text PRIMARY KEY,
    classification_description text,
    parent text
    )""")

import_export_table_create = ("""
    CREATE TABLE IF NOT EXISTS import_export_codes 
    (import_export_code text PRIMARY KEY,
    description text 
    )
    """)

trade_table_create = ("""
    CREATE TABLE IF NOT EXISTS trades
    (country_from text NOT NULL REFERENCES countries(country_id),
    country_to text NOT NULL REFERENCES countries(country_id),
    classification_code text NOT NULL REFERENCES classifications(classification_id),
    trade_time date NOT NULL ,
    import_export_code text NOT NULL,
    trade_weight DECIMAL(12,2),
    trade_value DECIMAL(12,2),
    PRIMARY KEY (country_from, country_to, classification_code, trade_time, import_export_code)
    )""")

cases_table_create = ("""
    CREATE TABLE IF NOT EXISTS cases
    (country_id text NOT NULL REFERENCES countries(country_id),
    period date NOT NULL,
    confirmed INT,
    deaths INT,
    recovered INT,
    active INT,
    PRIMARY KEY (country_id, period)
    )
    """)

create_table_queries = [country_table_create, classification_table_create,
                        import_export_table_create, trade_table_create, cases_table_create]

# INSERT TABLES
insert_import_export = ("""
    INSERT INTO import_export_codes
    (import_export_code, description)
    VALUES (%s, %s)
    ON CONFLICT (import_export_code) DO NOTHING;;
""")

insert_countries = ("""
    INSERT INTO countries
    (country_id, country_name, GDP_2017, GDP_per_Capita_2017, Population_2017, Military_Expenditure_2017)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (country_id) DO NOTHING;;
""")

insert_classification_codes = ("""
    INSERT INTO classifications
    (classification_id, parent, classification_description)
    VALUES (%s, %s, %s)
    ON CONFLICT (classification_id) DO NOTHING;
""")

insert_trades = ("""
    INSERT INTO trades
    (country_from, country_to, classification_code, trade_time, import_export_code, trade_weight, trade_value)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (country_from, country_to, classification_code, trade_time, import_export_code) DO NOTHING;
""")

insert_cases = ("""
    INSERT INTO cases
    (country_id, period, confirmed, deaths, recovered, active)
    VALUES (%s, %s, %s, %s, %s, %s)
""")



