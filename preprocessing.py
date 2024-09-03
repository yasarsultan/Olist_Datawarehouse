import pandas as  pd


def customers_dataset():
    df = pd.read_csv('rawOlist/customers_dataset.csv')
    df = df.dropna(subset=['customer_city', 'customer_state'])
    df = df.drop_duplicates(subset=['customer_id', 'customer_unique_id'])
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str)

    df.to_csv('olist/customers.csv', index=False)

def geolocation_dataset():
    df = pd.read_csv('rawOlist/geolocation_dataset.csv')
    df = df.dropna(subset=['geolocation_city', 'geolocation_state'])
    df = df.drop_duplicates(subset=['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'])
    df['geolocation_zip_code_prefix'] = df['geolocation_zip_code_prefix'].astype(str)
    df['geolocation_lat'] = df['geolocation_lat'].astype(float)
    df['geolocation_lng'] = df['geolocation_lng'].astype(float)

    df.to_csv('olist/geolocation.csv', index=False)

def order_items_dataset():
    df = pd.read_csv('rawOlist/order_items_dataset.csv')
    df = df.dropna(subset=['order_id', 'product_id', 'seller_id'])
    df = df.drop_duplicates(subset=['order_id', 'order_item_id'])
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['price'] = df['price'].astype(float)
    df['freight_value'] = df['freight_value'].astype(float)

    df.to_csv('olist/order_items.csv', index=False)

def order_payments_dataset():
    df = pd.read_csv('rawOlist/order_payments_dataset.csv')
    df = df.dropna(subset=['order_id', 'payment_type'])
    df = df.drop_duplicates(subset=['order_id', 'payment_sequential'])
    df['payment_installments'] = df['payment_installments'].astype(int)
    df['payment_value'] = df['payment_value'].astype(float)
    df = df[df['payment_value'] > 0]

    df.to_csv('olist/order_payments.csv', index=False)

def order_reviews_dataset():
    df = pd.read_csv('rawOlist/order_reviews_dataset.csv')
    df = df.dropna(subset=['order_id', 'review_score'])
    df = df.drop_duplicates(subset=['review_id'])
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    df.to_csv('olist/order_reviews.csv', index=False)

def orders_dataset():
    df = pd.read_csv('rawOlist/orders_dataset.csv')
    df = df.drop_duplicates(subset=['order_id'])
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df = df.dropna(subset=['order_id', 'customer_id', 'order_purchase_timestamp'])
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    df.to_csv('olist/orders.csv', index=False)

def products_dataset():
    df = pd.read_csv('rawOlist/products_dataset.csv')
    df = df.dropna(subset=['product_id', 'product_category_name'])
    df = df.drop_duplicates(subset=['product_id'])
    df['product_category_name'] = df['product_category_name'].astype(str)

    df.to_csv('olist/products.csv', index=False)

def sellers_dataset():
    df = pd.read_csv('rawOlist/sellers_dataset.csv')
    df = df.dropna(subset=['seller_id'])
    df = df.drop_duplicates(subset=['seller_id'])
    df['seller_zip_code_prefix'] = df['seller_zip_code_prefix'].astype(str)

    df.to_csv('olist/sellers.csv', index=False)

def main():
    customers_dataset()
    geolocation_dataset()
    order_items_dataset()
    order_payments_dataset()
    order_reviews_dataset()
    orders_dataset()
    products_dataset()
    sellers_dataset()

main()