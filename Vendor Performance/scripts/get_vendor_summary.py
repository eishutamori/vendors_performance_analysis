import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
filename = 'logs/get_vendor_summary.log',
level= logging.DEBUG,
format="%(asctime)s - %(levelname)s - %(message)s",
filemode= "a"
)

def  create_vendor_summary(con):
    '''this function will merge different table to get the overall vendor summary and adding new colum in resultant data'''
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS 
    (
        SELECT 
            VendorNumber, 
            SUM(Freight) as TotalFreightCost 
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    purchase_summary AS 
    (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price as ActualPrice,
            SUM(p.Quantity) as TotalPurchaseQuantity,
            SUM(p.Dollars) as TotalPurchaseAmount
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY 
            p.VendorNumber, p.VendorName, p.Brand, 
            p.Description, p.PurchasePrice, 
            pp.Volume, pp.Price
    ),

    Sales_Summary AS 
    (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(SalesDollars) as TotalSalesAmount,
            SUM(SalesPrice) as TotalSalesPrice,
            SUM(ExciseTax) as TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseAmount,

        ss.TotalSalesQuantity,
        ss.TotalSalesAmount,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,

        fs.TotalFreightCost

    FROM purchase_summary ps

    LEFT JOIN Sales_Summary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand

    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber

    ORDER BY ps.TotalPurchaseAmount DESC
    """, con)

    return vendor_sales_summary

def clean_data(df):
    '''this function will clean the data'''
    #changing data type to float
    df['Volume'] = df['Volume'].astype(float)

    #filling mssing values with 0
    df.fillna(0, inplace = True)

    #removing spaces form categorial columns
    df['VendorName'] = df['VendorName'].str.strip()

    #Creating New columns for better and additional analysis
    df['GrossProfit'] = df['TotalSalesAmount'] - df['TotalPurchaseAmount']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesAmount'])*100
    df['StockTurnover'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['TotalPurchaseRatio'] = df['TotalSalesAmount'] / df['TotalPurchaseAmount']

    return df 

if __name__ == '__main__':
    #creating database connection
    con = sqlite3.connect('inventory.db')

    logging.info('creating Vendor Summary Table...')
    summary_df = create_vendor_summary(con)
    logging.info(summary_df.head())

    logging.info('cleaning data...')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('ingesting data....')
    ingest_db(clean_df, 'vendor_sales_summary', con)
    logging.info('Completed')