--create database ETL_Star_Schema_DATA_WEARHOUSE_AdventureWorksLT2022;
--use ETL_Star_Schema_DATA_WEARHOUSE_AdventureWorksLT2022;
--use master;
--go
--drop database ETL_Star_Schema_DATA_WEARHOUSE_AdventureWorksLT2022;
--GO
drop table if exists DimCustomer, DimProduct, DimDate, FactSales;
-- DimCustomer (Type 2 SCD)
CREATE TABLE dbo.DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    CompanyName NVARCHAR(128) NULL,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL,
    AddressLine1 NVARCHAR(60) NULL,
    AddressLine2 NVARCHAR(60) NULL,
    City NVARCHAR(30) NULL,
    StateProvince NVARCHAR(50) NULL,
    CountryRegion NVARCHAR(50) NULL,
    PostalCode NVARCHAR(15) NULL,
    ValidFrom DATETIME2(7) NOT NULL,
    ValidTo DATETIME2(7) NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);
CREATE INDEX IX_DimCustomer_CustomerID ON dbo.DimCustomer(CustomerID);

-- DimProduct (Type 2 SCD)
CREATE TABLE dbo.DimProduct (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT NOT NULL,
    ProductName NVARCHAR(50) NOT NULL,
    ProductNumber NVARCHAR(25) NOT NULL,
    Color NVARCHAR(15) NULL,
    StandardCost MONEY NOT NULL,
    ListPrice MONEY NOT NULL,
    Size NVARCHAR(5) NULL,
    Weight DECIMAL(8,2) NULL,
    ProductCategoryID INT NULL,
    ProductCategoryName NVARCHAR(50) NULL,
    ProductModelName NVARCHAR(50) NULL,
    ValidFrom DATETIME2(7) NOT NULL,
    ValidTo DATETIME2(7) NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);
CREATE INDEX IX_DimProduct_ProductID ON dbo.DimProduct(ProductID);

-- DimDate
CREATE TABLE dbo.DimDate (
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    DayNumberOfWeek TINYINT NOT NULL,
    DayName NVARCHAR(10) NOT NULL,
    DayNumberOfMonth TINYINT NOT NULL,
    DayNumberOfYear SMALLINT NOT NULL,
    WeekNumberOfYear TINYINT NOT NULL,
    MonthName NVARCHAR(10) NOT NULL,
    MonthNumberOfYear TINYINT NOT NULL,
    CalendarQuarter TINYINT NOT NULL,
    CalendarYear SMALLINT NOT NULL,
    IsWeekend BIT NOT NULL
);

-- FactSales
CREATE TABLE dbo.FactSales (
    SalesOrderID INT NOT NULL,
    SalesOrderDetailID INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    OrderDateKey INT NOT NULL,
    DueDateKey INT NOT NULL,
    ShipDateKey INT NULL,
    OrderQty SMALLINT NOT NULL,
    UnitPrice MONEY NOT NULL,
    UnitPriceDiscount MONEY NOT NULL,
    LineTotal MONEY NOT NULL,
    PRIMARY KEY (SalesOrderID, SalesOrderDetailID),
    CONSTRAINT FK_FactSales_DimCustomer FOREIGN KEY (CustomerKey) REFERENCES dbo.DimCustomer(CustomerKey),
    CONSTRAINT FK_FactSales_DimProduct FOREIGN KEY (ProductKey) REFERENCES dbo.DimProduct(ProductKey),
    CONSTRAINT FK_FactSales_DimDate_OrderDate FOREIGN KEY (OrderDateKey) REFERENCES dbo.DimDate(DateKey),
    CONSTRAINT FK_FactSales_DimDate_DueDate FOREIGN KEY (DueDateKey) REFERENCES dbo.DimDate(DateKey),
    CONSTRAINT FK_FactSales_DimDate_ShipDate FOREIGN KEY (ShipDateKey) REFERENCES dbo.DimDate(DateKey)
);






----------------------------------------------------------------------------------------------------




-- Date dimension population
DECLARE @StartDate DATE = '2005-01-01';
DECLARE @EndDate DATE = '2025-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO dbo.DimDate (
        DateKey,
        FullDate,
        DayNumberOfWeek,
        DayName,
        DayNumberOfMonth,
        DayNumberOfYear,
        WeekNumberOfYear,
        MonthName,
        MonthNumberOfYear,
        CalendarQuarter,
        CalendarYear,
        IsWeekend
    )
    VALUES (
        CONVERT(INT, CONVERT(VARCHAR(8), @StartDate, 112)), -- DateKey as yyyyMMdd
        @StartDate, -- FullDate
        DATEPART(WEEKDAY, @StartDate), -- DayNumberOfWeek
        DATENAME(WEEKDAY, @StartDate), -- DayName
        DATEPART(DAY, @StartDate), -- DayNumberOfMonth
        DATEPART(DAYOFYEAR, @StartDate), -- DayNumberOfYear
        DATEPART(WEEK, @StartDate), -- WeekNumberOfYear
        DATENAME(MONTH, @StartDate), -- MonthName
        DATEPART(MONTH, @StartDate), -- MonthNumberOfYear
        DATEPART(QUARTER, @StartDate), -- CalendarQuarter
        DATEPART(YEAR, @StartDate), -- CalendarYear
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END -- IsWeekend
    );
    
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;


----------------------------------------------------------------------------------------------------




CREATE OR ALTER PROCEDURE dbo.usp_ETL_DimCustomer
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert new customers and update changed ones (SCD Type 2)
    INSERT INTO dbo.DimCustomer (
        CustomerID,
        CompanyName,
        FirstName,
        LastName,
        EmailAddress,
        Phone,
        AddressLine1,
        AddressLine2,
        City,
        StateProvince,
        CountryRegion,
        PostalCode,
        ValidFrom,
        ValidTo,
        IsCurrent
    )
    SELECT 
        c.CustomerID,
        c.CompanyName,
        c.FirstName,
        c.LastName,
        c.EmailAddress,
        c.Phone,
        a.AddressLine1,
        a.AddressLine2,
        a.City,
        a.StateProvince,
        a.CountryRegion,
        a.PostalCode,
        GETDATE() AS ValidFrom,
        NULL AS ValidTo,
        1 AS IsCurrent
    FROM AdventureWorksLT2022.SalesLT.Customer c
    LEFT JOIN AdventureWorksLT2022.SalesLT.CustomerAddress ca ON c.CustomerID = ca.CustomerID
    LEFT JOIN AdventureWorksLT2022.SalesLT.Address a ON ca.AddressID = a.AddressID
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.DimCustomer dc 
        WHERE dc.CustomerID = c.CustomerID AND dc.IsCurrent = 1
    )
    OR EXISTS (
        SELECT 1 FROM dbo.DimCustomer dc 
        WHERE dc.CustomerID = c.CustomerID 
        AND dc.IsCurrent = 1
        AND (
            ISNULL(dc.CompanyName, '') <> ISNULL(c.CompanyName, '')
            OR dc.FirstName <> c.FirstName
            OR dc.LastName <> c.LastName
            OR ISNULL(dc.EmailAddress, '') <> ISNULL(c.EmailAddress, '')
            OR ISNULL(dc.Phone, '') <> ISNULL(c.Phone, '')
            -- Add other fields to compare
        )
    );
    
    -- Expire changed records
    UPDATE dc
    SET 
        dc.ValidTo = GETDATE(),
        dc.IsCurrent = 0
    FROM dbo.DimCustomer dc
    INNER JOIN AdventureWorksLT2022.SalesLT.Customer c ON dc.CustomerID = c.CustomerID
    WHERE dc.IsCurrent = 1
    AND (
        ISNULL(dc.CompanyName, '') <> ISNULL(c.CompanyName, '')
        OR dc.FirstName <> c.FirstName
        OR dc.LastName <> c.LastName
        OR ISNULL(dc.EmailAddress, '') <> ISNULL(c.EmailAddress, '')
        OR ISNULL(dc.Phone, '') <> ISNULL(c.Phone, '')
        -- Add other fields to compare
    );
END;
GO




CREATE OR ALTER PROCEDURE dbo.usp_ETL_DimProduct
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert new products and update changed ones (SCD Type 2)
    INSERT INTO dbo.DimProduct (
        ProductID,
        ProductName,
        ProductNumber,
        Color,
        StandardCost,
        ListPrice,
        Size,
        Weight,
        ProductCategoryID,
        ProductCategoryName,
        ProductModelName,
        ValidFrom,
        ValidTo,
        IsCurrent
    )
    SELECT 
        p.ProductID,
        p.Name,
        p.ProductNumber,
        p.Color,
        p.StandardCost,
        p.ListPrice,
        p.Size,
        p.Weight,
        p.ProductCategoryID,
        pc.Name AS ProductCategoryName,
        pm.Name AS ProductModelName,
        GETDATE() AS ValidFrom,
        NULL AS ValidTo,
        1 AS IsCurrent
    FROM AdventureWorksLT2022.SalesLT.Product p
    LEFT JOIN AdventureWorksLT2022.SalesLT.ProductCategory pc ON p.ProductCategoryID = pc.ProductCategoryID
    LEFT JOIN AdventureWorksLT2022.SalesLT.ProductModel pm ON p.ProductModelID = pm.ProductModelID
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.DimProduct dp 
        WHERE dp.ProductID = p.ProductID AND dp.IsCurrent = 1
    )
    OR EXISTS (
        SELECT 1 FROM dbo.DimProduct dp 
        WHERE dp.ProductID = p.ProductID 
        AND dp.IsCurrent = 1
        AND (
            dp.ProductName <> p.Name
            OR dp.ProductNumber <> p.ProductNumber
            OR ISNULL(dp.Color, '') <> ISNULL(p.Color, '')
            OR dp.StandardCost <> p.StandardCost
            OR dp.ListPrice <> p.ListPrice
            -- Add other fields to compare
        )
    );
    
    -- Expire changed records
    UPDATE dp
    SET 
        dp.ValidTo = GETDATE(),
        dp.IsCurrent = 0
    FROM dbo.DimProduct dp
    INNER JOIN AdventureWorksLT2022.SalesLT.Product p ON dp.ProductID = p.ProductID
    WHERE dp.IsCurrent = 1
    AND (
        dp.ProductName <> p.Name
        OR dp.ProductNumber <> p.ProductNumber
        OR ISNULL(dp.Color, '') <> ISNULL(p.Color, '')
        OR dp.StandardCost <> p.StandardCost
        OR dp.ListPrice <> p.ListPrice
        -- Add other fields to compare
    );
END;
GO



CREATE OR ALTER PROCEDURE dbo.usp_ETL_FactSales
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Clear existing data (incremental load would be better for production)
    TRUNCATE TABLE dbo.FactSales;
    
    -- Insert sales facts
    INSERT INTO dbo.FactSales (
        SalesOrderID,
        SalesOrderDetailID,
        CustomerKey,
        ProductKey,
        OrderDateKey,
        DueDateKey,
        ShipDateKey,
        OrderQty,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal
    )
    SELECT 
        sod.SalesOrderID,
        sod.SalesOrderDetailID,
        dc.CustomerKey,
        dp.ProductKey,
        CONVERT(INT, CONVERT(VARCHAR(8), so.OrderDate, 112)) AS OrderDlateKey,
        CONVERT(INT, CONVERT(VARCHAR(8), so.DueDate, 112)) AS DueDateKey,
        CONVERT(INT, CONVERT(VARCHAR(8), so.ShipDate, 112)) AS ShipDateKey,
        sod.OrderQty,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        sod.LineTotal
    FROM AdventureWorksLT2022.SalesLT.SalesOrderDetail sod
    INNER JOIN AdventureWorksLT2022.SalesLT.SalesOrderHeader so ON sod.SalesOrderID = so.SalesOrderID
    INNER JOIN dbo.DimCustomer dc ON so.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
    INNER JOIN dbo.DimProduct dp ON sod.ProductID = dp.ProductID AND dp.IsCurrent = 1;
END;
GO
--select so.OrderDate, CONVERT(INT, CONVERT(VARCHAR(8), so.OrderDate, 112)) AS OrderDlateKey1,
--CONVERT(VARCHAR(8), so.OrderDate, 112) AS OrderDlateKey1  from AdventureWorksLT2022.SalesLT.SalesOrderHeader so



CREATE OR ALTER PROCEDURE dbo.usp_ETL_FactSales
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Clear existing data (incremental load would be better for production)
    TRUNCATE TABLE dbo.FactSales;
    
    -- Insert sales facts
    INSERT INTO dbo.FactSales (
        SalesOrderID,
        SalesOrderDetailID,
        CustomerKey,
        ProductKey,
        OrderDateKey,
        DueDateKey,
        ShipDateKey,
        OrderQty,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal
    )
    SELECT 
        sod.SalesOrderID,
        sod.SalesOrderDetailID,
        dc.CustomerKey,
        dp.ProductKey,
        CONVERT(INT, CONVERT(VARCHAR(8), so.OrderDate, 112)) AS OrderDateKey,
        CONVERT(INT, CONVERT(VARCHAR(8), so.DueDate, 112)) AS DueDateKey,
        CONVERT(INT, CONVERT(VARCHAR(8), so.ShipDate, 112)) AS ShipDateKey,
        sod.OrderQty,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        sod.LineTotal
    FROM AdventureWorksLT2022.SalesLT.SalesOrderDetail sod
    INNER JOIN AdventureWorksLT2022.SalesLT.SalesOrderHeader so ON sod.SalesOrderID = so.SalesOrderID
    INNER JOIN dbo.DimCustomer dc ON so.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
    INNER JOIN dbo.DimProduct dp ON sod.ProductID = dp.ProductID AND dp.IsCurrent = 1;
END;
GO

--select * from AdventureWorksLT2022.SalesLT.SalesOrderHeader



CREATE OR ALTER procedure dbo.usp_ETL_Master
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Execute dimension ETLs
        EXEC dbo.usp_ETL_DimCustomer;
        EXEC dbo.usp_ETL_DimProduct;
        
        -- Execute fact ETL
        EXEC dbo.usp_ETL_FactSales;
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        THROW;
    END CATCH;
END;
GO



 --Run the complete ETL process
EXEC dbo.usp_ETL_Master;


select * from DimCustomer;
select * from DimDate;
select * from DimProduct;
select * from FactSales;


select d.DateKey,f.ShipDateKey from DimDate d, FactSales f
where d.DateKey = f.ShipDateKey





