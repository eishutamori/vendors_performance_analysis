# vendors_performance_analysis

End-to-end data analysis project using Python, SQL, and Power BI to evaluate vendor performance, optimize inventory, and drive business insights.

**Note: Data files were not uploaded as they were wery large in size for github.
**
This project analyzes vendor performance, profitability, and inventory efficiency using Python, SQL, and Power BI.

**Objectives:**
Identify top and low-performing vendors
Analyze sales and profit trends
Detect unsold inventory and capital lock
Find high-margin low-sales brands

**Tools Used:**
Python (Pandas, NumPy)
SQL (SQLite)
Power BI
Matplotlib / Seaborn

**Key Insights**
Initially $9.5M was shown as unsold inventory
After fixing calculation, actual unsold capital is ~$2.7M
Some vendors contribute heavily to unsold inventory
High-margin low-sales brands identified for promotion
Freight cost shows large variation

**Important Learning**

Sales were sometimes higher than purchases due to old stock.
To fix this, negative unsold inventory values were set to 0.
