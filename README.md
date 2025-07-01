# -T-SQL-_ETL_Star_Schema_DATA_WEARHOUSE_AdventureWorksLT2022

# ETL Star Schema Data Warehouse - AdventureWorksLT2022

This project implements a **Data Warehouse** with a **Star Schema** in Microsoft SQL Server, using the **AdventureWorksLT2022** sample database as the source. The warehouse includes **Slowly Changing Dimension (SCD) Type 2** handling, a Date Dimension, and a fully modeled Fact table for sales analysis.

---

## 📌 Features

- **Star Schema** with fact and dimension tables
- **SCD Type 2** implementation for tracking historical changes in DimCustomer and DimProduct
- **Date Dimension** with full date attributes
- **ETL Stored Procedures** for repeatable loading
- Supports incremental history loading for dimensions
- Clean SQL Server-based implementation

---

## 🗺️ Architecture Overview

**Schema:**
- Tables created in the `dbo` schema

**Dimensions:**
- `DimCustomer` (Type 2 SCD)
- `DimProduct` (Type 2 SCD)
- `DimDate` (fully populated date dimension)

**Fact Table:**
- `FactSales` with foreign keys to all dimensions

---

## 🛠️ Project Structure

**Tables Created:**
- `DimCustomer`
- `DimProduct`
- `DimDate`
- `FactSales`

**Stored Procedures:**
- `usp_ETL_DimCustomer`: Loads and manages Type 2 changes for customers
- `usp_ETL_DimProduct`: Loads and manages Type 2 changes for products
- `usp_ETL_FactSales`: Loads sales fact data
- `usp_ETL_Master`: Orchestrates the entire ETL pipeline

---

## ⚡ How It Works

1️⃣ **Create Dimensions**
- `DimDate` is pre-populated over a date range (e.g., 2005–2025)
- `DimCustomer` and `DimProduct` use Type 2 SCD logic to track history
  - New records are inserted
  - Changed records are expired (ValidTo set) and new versions inserted

2️⃣ **Load Fact Table**
- FactSales links to dimensions using foreign keys
- Includes order dates, quantities, pricing, discounts

3️⃣ **ETL Orchestration**
- `usp_ETL_Master` procedure runs all dimension and fact loads in a single transaction

---

## 🗂️ Tables and Entities

### DimCustomer (Type 2 SCD)
- Tracks history of customer changes
- Columns include ValidFrom, ValidTo, IsCurrent
- Supports querying customer attributes as of any point in time

### DimProduct (Type 2 SCD)
- Tracks changes in product details, pricing, categorization
- Historical tracking of changes over time

### DimDate
- Standard calendar dimension
- Includes day/week/month/quarter/year fields
- Populated for full date range

### FactSales
- Measures sales transactions
- Contains foreign keys to all dimensions
- Includes order quantity, pricing, discounts, computed line totals

---

## 💻 Usage

1️⃣ **Prerequisites**
- Microsoft SQL Server (2017+ recommended)
- Existing `AdventureWorksLT2022` database with:
  - SalesLT.Customer
  - SalesLT.Product
  - SalesLT.SalesOrderHeader
  - SalesLT.SalesOrderDetail
  - SalesLT.Address tables populated

2️⃣ **Run the Scripts**
- Create the warehouse database (optional)
- Run table creation scripts
- Populate `DimDate`
- Create stored procedures

3️⃣ **Execute the ETL Pipeline**
```sql
EXEC dbo.usp_ETL_Master;
