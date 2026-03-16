# ⭐ Gold Layer – Data Catalog

## Overview
The **Gold Layer** represents the **business-ready analytical layer** of the data warehouse.  
It is designed using a **Star Schema** to support **analytics, dashboards, and BI reporting**.

This layer contains:

- **Dimension Tables** → descriptive business entities  
- **Fact Table** → transactional sales metrics

---

# 👥 Dimension: dim_customers

### Description
Contains **customer demographic and profile information** used for segmentation and customer analytics.

**Primary Key:** `customer_key`

| Column | Description |
|------|-------------|
| `customer_key` | Surrogate primary key |
| `customer_id` | Customer ID from source system |
| `customer_number` | Business customer identifier |
| `first_name` | Customer first name |
| `last_name` | Customer last name |
| `birthdate` | Customer date of birth |
| `country` | Country of residence |
| `marital_status` | Customer marital status |
| `gender` | Customer gender |
| `create_date` | Date when the customer record was created |

---

# 📦 Dimension: dim_products

### Description
Contains **product information and category hierarchy** used to analyze product performance and sales distribution.

**Primary Key:** `product_key`

| Column | Description |
|------|-------------|
| `product_key` | Surrogate primary key |
| `product_id` | Product ID from source system |
| `product_number` | Business product identifier |
| `product_name` | Product name |
| `product_line` | Product line classification |
| `category_id` | Product category identifier |
| `category` | Product category name |
| `subcategory` | Product subcategory |
| `maintenance` | Indicates maintenance requirement |
| `cost` | Product cost |
| `start_date` | Product availability start date |

---

# 💰 Fact Table: fact_sales

### Description
Stores **transactional sales data and quantitative business metrics** used for analytical reporting.

### Primary Key
`order_number`

### Foreign Keys

| Column | References |
|------|-------------|
| `customer_key` | `gold.dim_customers` |
| `product_key` | `gold.dim_products` |

| Column | Description |
|------|-------------|
| `order_number` | Unique sales order identifier |
| `customer_key` | Customer reference |
| `product_key` | Product reference |
| `order_date` | Date the order was placed |
| `ship_date` | Date the order was shipped |
| `due_date` | Expected delivery date |
| `sales_amount` | Total sales value |
| `quantity` | Number of units sold |
| `price` | Unit price |


---

# 🔗 Table Relationships

dim_customers (1) ────────< fact_sales >──────── (1) dim_products

- One **customer** can generate **many sales transactions**
- One **product** can appear in **many sales transactions**
