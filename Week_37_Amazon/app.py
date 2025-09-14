import polars as pl
import polars.selectors as cs
import os

#  Dataset has 10 unique customers & locations, 92 unique customer/locatio pairs
# 
#      radio buttons pick group by customer or by location
#      radio buttons analysis of total amount, method count or status count
#      using group by variable, make 
#           horizontal pareto bar group_by (y), analysis parameter (x) 
#      timeline 
#           date on x-axis, analysis parameter on y, one graph per group by
#      tree map of selected group_by, analysis parameter 


if 'df.parquet' not in os.listdir():
    print('reading data from csv file')
    #----- SETUP ENUMERATED TYPES --------------------------------------------------
    product_enum = pl.Enum(
        ['Book', 'Headphones', 'Jeans', 'Laptop', 'Refrigerator', 'Running Shoes', 
        'Smartphone', 'Smartwatch', 'T-Shirt', 'Washing Machine']
    )
    category_enum =  pl.Enum(
        ['Books', 'Clothing', 'Electronics', 'Footwear', 'Home Appliances']
    )
    customer_enum = pl.Enum(
        ['Chris White', 'Daniel Harris', 'David Lee', 'Emily Johnson', 'Emma Clark', 
        'Jane Smith', 'John Doe', 'Michael Brown', 'Olivia Wilson', 'Sophia Miller']
    )
    location_enum = pl.Enum(
        ['Boston', 'Chicago', 'Dallas', 'Denver', 'Houston', 'Los Angeles', 'Miami', 
        'New York', 'San Francisco', 'Seattle']
    )
    payment_method_enum = pl.Enum(
        ['Amazon Pay', 'Credit Card', 'Debit Card', 'Gift Card', 'PayPal']
    )
    status_enum=pl.Enum(['Cancelled', 'Completed', 'Pending'])

    #----- LOAD AND CLEAN THE DATASET
    df  = (
        pl.read_csv(
            'amazon_sales_data.csv',
        )
        .rename(  # upper case all column names, replace spaces with underscores
            lambda c: 
                c.upper()            # column names to upper case
                .replace(' ', '_')   # blanks replaced with underscores
        )
        .with_columns(      # clean up DATE column and convert to pl.Date
            DATE = pl.col('DATE')
                .str.replace_all('-', '/')
                .str.replace_all('/25', '/2025')
                .str.to_date(format='%d/%m/%Y')
        )
        .rename({
            'ORDER_ID'           : 'ID',
            'CUSTOMER_NAME'      : 'CUSTOMER',
            'TOTAL_SALES'        : 'TOTAL',
            'CUSTOMER_LOCATION'  : 'LOCATION',
            'PAYMENT_METHOD'     : 'METHOD'
        })
        .with_columns(
            pl.col(['PRICE','TOTAL']).cast(pl.UInt16),
            pl.col('ID').str.slice(3).cast(pl.UInt8),
            pl.col('PRODUCT').cast(product_enum),
            pl.col('CATEGORY').cast(category_enum),       
            pl.col('CUSTOMER').cast(customer_enum),
            pl.col('LOCATION').cast(location_enum),
            pl.col('METHOD').cast(payment_method_enum),
            pl.col('STATUS').cast(status_enum),
            pl.col('QUANTITY').cast(pl.UInt8),
        )    
    )
    df.glimpse()
    df.write_parquet('df.parquet')
else:
    print('reading data from parquet file')
    df = pl.read_parquet('df.parquet')

print(df.glimpse())

