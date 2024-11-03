-- creo un esquiema en postgres llamado golden
CREATE SCHEMA golden;


CREATE TABLE golden.securities (
    id_securitie SERIAL PRIMARY KEY,
    ticket VARCHAR(10),
    full_name VARCHAR(100),
    securitie_type VARCHAR(50),
    financial_instrument_type VARCHAR(50),
    dividend_yield FLOAT,
    par_value FLOAT
);

-- creo o reemplazo la tabla golden.portfolios
CREATE TABLE golden.portfolios (
    id_portfolio SERIAL PRIMARY KEY,
    total_investment FLOAT,
    partition_date CHAR(10)
);

-- creo o reemplazo la tabla stocks
CREATE TABLE golden.stocks (
    id_stock SERIAL PRIMARY KEY,
    id_portfolio INT,
    id_securitie INT,
    average_purchase_price FLOAT,
    quantity FLOAT,
    last_price FLOAT,
    profit_loss_percent FLOAT,
    dividend_yield FLOAT,
    interest_paid FLOAT,
    amortization_paid FLOAT,
    partition_date VARCHAR(10),
    FOREIGN KEY (id_securitie) REFERENCES golden.securities(id_securitie),
    FOREIGN KEY (id_portfolio) REFERENCES golden.portfolios(id_portfolio)
);

-- elimino la tabla stocks y portfolio
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS portfolio;