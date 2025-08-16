import csv
import json
import urllib.request
import requests
import pandas as pd
import argparse
import time
import sys

from bs4 import BeautifulSoup

#shopify_scraper made for DeepSolve Innovation Intership Application by Mathesh Natesh

parser = argparse.ArgumentParser(description="Scrap products data from Shopify store")
parser.add_argument('-t', '--target', dest='website_url', type=str, help='URL to Shopify store (https://shopifystore.com)')
parser.add_argument('-v', '--variants', dest='variants', action="store_true", help='Scrap also with variants data')
args = parser.parse_args()

if not args.website_url:
    print("usage: shopify_scraper.py [-h] [-t WEBSITE_URL] [-v]")
    exit(0)

base_url = args.website_url.rstrip('/')  # Remove trailing slash
url = base_url + '/products.json'
with_variants = args.variants

print(f"[+] Target URL: {base_url}")
print(f"[+] Products JSON URL: {url}")
print(f"[+] Include variants: {with_variants}")

def get_page(page):
    try:
        page_url = url + '?page={}'.format(page)
        print(f"[DEBUG] Fetching: {page_url}")
        
        data = urllib.request.urlopen(page_url).read()
        json_data = json.loads(data)
        products = json_data.get('products', [])
        
        print(f"[DEBUG] Found {len(products)} products on page {page}")
        return products
    except urllib.error.HTTPError as e:
        print(f"[ERROR] HTTP Error {e.code}: {e.reason}")
        return []
    except urllib.error.URLError as e:
        print(f"[ERROR] URL Error: {e.reason}")
        return []
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON decode error: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return []

def get_tags_from_product(product):
    try:
        r = urllib.request.urlopen(product).read()
        soup = BeautifulSoup(r, "html.parser")

        title = soup.title.string if soup.title else ''
        description = ''

        meta = soup.find_all('meta')
        for tag in meta:
            if 'name' in tag.attrs.keys() and tag.attrs['name'].strip().lower() == 'description':
                description = tag.attrs['content']
                break

        return [title, description]
    except Exception as e:
        print(f"[ERROR] Error getting tags from {product}: {e}")
        return ['', '']

def get_inventory_from_product(product_url):
    try:
        get_product = requests.get(product_url, timeout=10)
        get_product.raise_for_status()  # Raise exception for bad status codes
        product_json = get_product.json()
        
        if 'product' in product_json and 'variants' in product_json['product']:
            product_variants = pd.DataFrame(product_json['product']['variants'])
            return product_variants
        else:
            print(f"[ERROR] No variants found in product JSON")
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request error for {product_url}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] Error getting inventory from {product_url}: {e}")
        return pd.DataFrame()

# Test the initial connection
print("[+] Testing connection to products endpoint...")
try:
    test_data = urllib.request.urlopen(url).read()
    test_json = json.loads(test_data)
    if 'products' in test_json:
        print(f"[+] Connection successful! Found {len(test_json['products'])} products on first page")
    else:
        print("[ERROR] No 'products' key found in JSON response")
        print(f"[DEBUG] Response keys: {list(test_json.keys())}")
        sys.exit(1)
except Exception as e:
    print(f"[ERROR] Failed to connect to {url}: {e}")
    sys.exit(1)

with open('products.csv', 'w', newline='', encoding='utf-8') as f:
    page = 1
    total_products = 0

    print("[+] Starting script")

    # create file header
    writer = csv.writer(f)
    if with_variants:
        writer.writerow([
            'Name', 'Variant ID', 'Product ID', 'Variant Title', 'Price', 'SKU', 
            'Position', 'Inventory Policy', 'Compare At Price', 'Fulfillment Service',
            'Inventory Management', 'Option1', 'Option2', 'Option3', 'Created At',
            'Updated At', 'Taxable', 'Barcode', 'Grams', 'Image ID', 'Weight',
            'Weight Unit', 'Inventory Quantity', 'Old Inventory Quantity',
            'Tax Code', 'Requires Shipping', 'Quantity Rule', 'Price Currency',
            'Compare At Price Currency', 'Quantity Price Breaks',
            'URL', 'Meta Title', 'Meta Description', 'Product Description'
        ])
    else:
        writer.writerow(['Name', 'URL', 'Meta Title', 'Meta Description', 'Product Description'])

    print("[+] Checking products pages...")

    products = get_page(page)
    while products:
        print(f"[+] Processing page {page} with {len(products)} products")
        
        for i, product in enumerate(products):
            name = product.get('title', 'Unknown')
            handle = product.get('handle', '')
            product_url = base_url + '/products/' + handle
            body_html = product.get('body_html', '')

            body_description = BeautifulSoup(body_html, "html.parser").get_text() if body_html else ''

            print(f" ├ [{i+1}/{len(products)}] Scraping: {name}")

            title, description = get_tags_from_product(product_url)

            if with_variants:
                variants_df = get_inventory_from_product(product_url + '.json')
                if not variants_df.empty:
                    for _, variant in variants_df.iterrows():
                        row = [
                            name, variant.get('id', ''), variant.get('product_id', ''), 
                            variant.get('title', ''), variant.get('price', ''), 
                            variant.get('sku', ''), variant.get('position', ''),
                            variant.get('inventory_policy', ''), variant.get('compare_at_price', ''),
                            variant.get('fulfillment_service', ''), variant.get('inventory_management', ''),
                            variant.get('option1', ''), variant.get('option2', ''), 
                            variant.get('option3', ''), variant.get('created_at', ''),
                            variant.get('updated_at', ''), variant.get('taxable', ''),
                            variant.get('barcode', ''), variant.get('grams', ''), 
                            variant.get('image_id', ''), variant.get('weight', ''),
                            variant.get('weight_unit', ''), variant.get('inventory_quantity', ''),
                            variant.get('old_inventory_quantity', ''), variant.get('tax_code', ''),
                            variant.get('requires_shipping', ''), variant.get('quantity_rule', ''),
                            variant.get('price_currency', ''), variant.get('compare_at_price_currency', ''),
                            variant.get('quantity_price_breaks', ''),
                            product_url, title, description, body_description
                        ]
                        writer.writerow(row)
                else:
                    print(f"   └ No variants found for {name}")
            else:
                row = [name, product_url, title, description, body_description]
                writer.writerow(row)
            
            total_products += 1
            
            # Add a small delay to be respectful to the server
            time.sleep(0.1)

        page += 1
        products = get_page(page)
        
        # Safety check to prevent infinite loops
        if page > 100:
            print("[WARNING] Reached page limit (100). Stopping to prevent infinite loop.")
            break

    print(f"[+] Scraping completed! Total products processed: {total_products}")
    print(f"[+] Results saved to products.csv")