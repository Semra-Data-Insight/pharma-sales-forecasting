CREATE DATABASE PharmaSalesDB;
GO
USE PharmaSalesDB;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg')  EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='core') EXEC('CREATE SCHEMA core');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='mart') EXEC('CREATE SCHEMA mart');
GO
PRINT 'Schemas created.';
CREATE TABLE stg.PharmaceuticalSales_raw (
    sale_id             INT            NOT NULL,
    [date]              DATE           NOT NULL,
    company             NVARCHAR(100)  NOT NULL,
    product             NVARCHAR(100)  NOT NULL,
    atc_class           NVARCHAR(60)   NOT NULL,
    rx_otc              VARCHAR(3)     NOT NULL,
    form                NVARCHAR(30)   NOT NULL,
    strength_mg         INT            NULL,
    package_size        INT            NULL,
    channel             NVARCHAR(20)   NOT NULL,
    region              NVARCHAR(30)   NOT NULL,
    city                NVARCHAR(50)   NOT NULL,
    sales_rep           NVARCHAR(10)   NULL,
    doctor_specialty    NVARCHAR(50)   NULL,
    units_sold          INT            NOT NULL,
    unit_price_try      DECIMAL(10,2)  NOT NULL,
    discount_rate       DECIMAL(5,3)   NOT NULL,
    gross_revenue_try   DECIMAL(12,2)  NOT NULL,
    net_revenue_try     DECIMAL(12,2)  NOT NULL,
    prescription_count  INT            NOT NULL,
    patient_age_group   NVARCHAR(10)   NOT NULL
);
BULK INSERT stg.PharmaceuticalSales_raw
FROM 'C:\pharma_sales_tr_2024_2025_300rows.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',
    TABLOCK
);
IF OBJECT_ID('stg.v_clean_pharma_basic') IS NOT NULL
    DROP VIEW stg.v_clean_pharma_basic;
GO

CREATE VIEW stg.v_clean_pharma_basic AS
SELECT
    sale_id,
    [date],
    company,
    product,
    atc_class,
    rx_otc,
    form,
    TRY_CONVERT(INT, strength_mg)         AS strength_mg,
    TRY_CONVERT(INT, package_size)        AS package_size,
    channel,
    region,
    city,
    sales_rep,
    doctor_specialty,
    TRY_CONVERT(INT, units_sold)          AS units_sold,
    TRY_CONVERT(DECIMAL(10,2), unit_price_try) AS unit_price_try,
    TRY_CONVERT(DECIMAL(5,3), discount_rate)   AS discount_rate,
    TRY_CONVERT(DECIMAL(12,2), gross_revenue_try) AS gross_revenue_try,
    TRY_CONVERT(DECIMAL(12,2), net_revenue_try)   AS net_revenue_try,
    TRY_CONVERT(INT, prescription_count)  AS prescription_count,
    patient_age_group
FROM stg.PharmaceuticalSales_raw;
GO
SELECT *FROM stg.v_clean_pharma_basic;
SELECT * FROM stg.PharmaceuticalSales_raw;
IF OBJECT_ID('core.PharmaceuticalSales') IS NOT NULL
    DROP TABLE core.PharmaceuticalSales;
GO

CREATE TABLE core.PharmaceuticalSales (
    sale_id INT,
    [date] DATE,
    company NVARCHAR(100),
    product NVARCHAR(100),
    atc_class NVARCHAR(100),
    rx_otc NVARCHAR(10),
    form NVARCHAR(30),
    strength_mg INT,
    package_size INT,
    channel NVARCHAR(30),
    region NVARCHAR(30),
    city NVARCHAR(50),
    sales_rep NVARCHAR(20),
    doctor_specialty NVARCHAR(50),
    units_sold INT,
    unit_price_try DECIMAL(10,2),
    discount_rate DECIMAL(5,3),
    gross_revenue_try DECIMAL(12,2),
    net_revenue_try DECIMAL(12,2),
    prescription_count INT,
    patient_age_group NVARCHAR(10)
);
GO

INSERT INTO core.PharmaceuticalSales
SELECT * FROM stg.v_clean_pharma_basic;
GO
SELECT COUNT(*) AS row_count FROM core.PharmaceuticalSales;
SELECT * FROM core.PharmaceuticalSales;
USE PharmaSalesDB;
GO
IF OBJECT_ID('mart.v_sales_monthly') IS NOT NULL DROP VIEW mart.v_sales_monthly;
GO
CREATE VIEW mart.v_sales_monthly AS
SELECT
    CAST(DATEFROMPARTS(YEAR([date]), MONTH([date]), 1) AS DATE) AS month_start,
    YEAR([date])  AS [year],
    MONTH([date]) AS [month_no],
    DATENAME(MONTH, [date]) AS [month_name],
    SUM(net_revenue_try)   AS net_revenue,
    SUM(gross_revenue_try) AS gross_revenue,
    SUM(units_sold)        AS units,
    AVG(unit_price_try)    AS avg_unit_price,
    AVG(discount_rate)     AS avg_discount
FROM core.PharmaceuticalSales
GROUP BY
    DATEFROMPARTS(YEAR([date]), MONTH([date]), 1),
    YEAR([date]),
    MONTH([date]),
    DATENAME(MONTH, [date]);
GO
IF OBJECT_ID('mart.v_sales_by_channel') IS NOT NULL DROP VIEW mart.v_sales_by_channel;
GO
CREATE VIEW mart.v_sales_by_channel AS
SELECT
    CAST(DATEFROMPARTS(YEAR([date]), MONTH([date]), 1) AS DATE) AS month_start,
    channel,
    SUM(net_revenue_try)   AS net_revenue,
    SUM(gross_revenue_try) AS gross_revenue,
    SUM(units_sold)        AS units,
    AVG(unit_price_try)    AS avg_unit_price,
    AVG(discount_rate)     AS avg_discount,
    CAST(
      SUM(net_revenue_try) * 1.0 /
      NULLIF(SUM(SUM(net_revenue_try)) OVER (PARTITION BY DATEFROMPARTS(YEAR([date]), MONTH([date]), 1)), 0)
      AS DECIMAL(6,4)
    ) AS net_share
FROM core.PharmaceuticalSales
GROUP BY
    DATEFROMPARTS(YEAR([date]), MONTH([date]), 1),
    channel;
GO
IF OBJECT_ID('mart.v_top_products') IS NOT NULL DROP VIEW mart.v_top_products;
GO
CREATE VIEW mart.v_top_products AS
SELECT
    product,
    atc_class,
    rx_otc,
    SUM(net_revenue_try)   AS total_net_revenue,
    SUM(gross_revenue_try) AS total_gross_revenue,
    SUM(units_sold)        AS total_units,
    AVG(unit_price_try)    AS avg_unit_price,
    AVG(discount_rate)     AS avg_discount
FROM core.PharmaceuticalSales
GROUP BY product, atc_class, rx_otc;
GO
SELECT TOP (12) * FROM mart.v_sales_monthly ORDER BY month_start;

SELECT TOP (12) * FROM mart.v_sales_by_channel ORDER BY month_start, net_share DESC;

SELECT TOP (10) *
FROM mart.v_top_products
ORDER BY total_net_revenue DESC;
