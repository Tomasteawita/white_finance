-- creo un esquiema en postgres llamado golden
CREATE SCHEMA golden;

/* creo una tabla en el esquema golden llamada securities
Con los siguientes campos:
- id_securitie INT PK
- ticket STR
- full_name STR
- securitie_type STR
- Financial-instrument_type STR
- dividend_yield FLOAT
- par_value FLOAT
*/

CREATE TABLE golden.securities (
    id_securitie INT PRIMARY KEY,
    ticket VARCHAR(10),
    full_name VARCHAR(100),
    securitie_type VARCHAR(50),
    financial_instrument_type VARCHAR(50),
    dividend_yield FLOAT,
    par_value FLOAT
);