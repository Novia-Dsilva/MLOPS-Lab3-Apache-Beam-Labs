

"""
E-Commerce Analytics Pipeline using Apache Beam
Processes Amazon Sales Dataset

Dataset columns: product_id, product_name, category, discounted_price, 
actual_price, discount_percentage, rating, rating_count, about_product, 
user_id, user_name, review_id, review_title, review_content, img_link, product_link
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import os
import re

# ==================== HELPER FUNCTIONS ====================

def parse_csv_line(line):
    """Parse CSV line from Amazon dataset into dictionary"""
    if line.startswith('product_id'):  # Skip header
        return None
    
    try:
        # Use csv reader to handle quoted fields with commas
        reader = csv.reader([line])
        parts = next(reader)
        
        if len(parts) < 7:
            return None
        
        # Clean price values (remove ₹, commas)
        def clean_price(price_str):
            if not price_str or price_str.strip() == '':
                return 0.0
            cleaned = price_str.replace('₹', '').replace(',', '').strip()
            try:
                return float(cleaned)
            except:
                return 0.0
        
        # Clean percentage (remove %)
        def clean_percentage(pct_str):
            if not pct_str or pct_str.strip() == '':
                return 0.0
            cleaned = pct_str.replace('%', '').strip()
            try:
                return float(cleaned)
            except:
                return 0.0
        
        # Clean rating
        def clean_rating(rating_str):
            if not rating_str or rating_str.strip() == '':
                return 0.0
            try:
                return float(rating_str.strip())
            except:
                return 0.0
        
        # Clean rating count (remove commas)
        def clean_rating_count(count_str):
            if not count_str or count_str.strip() == '':
                return 0
            cleaned = count_str.replace(',', '').strip()
            try:
                return int(cleaned)
            except:
                return 0
        
        product_id = parts[0].strip()
        product_name = parts[1].strip()
        category = parts[2].strip()
        discounted_price = clean_price(parts[3])
        actual_price = clean_price(parts[4])
        discount_percentage = clean_percentage(parts[5])
        rating = clean_rating(parts[6])
        rating_count = clean_rating_count(parts[7]) if len(parts) > 7 else 0
        
        # Skip invalid entries
        if discounted_price <= 0:
            return None
        
        # Simulate quantity per order (1-3 items typical)
        import random
        quantity = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
        
        # Generate customer ID from user_ids if available
        customer_id = f"CUST{random.randint(1000, 9999)}"
        if len(parts) > 9 and parts[9].strip():
            # Use first user_id if available
            user_ids = parts[9].split(',')
            if user_ids:
                customer_id = user_ids[0].strip()
        
        return {
            'product_id': product_id,
            'product_name': product_name[:100],  # Truncate long names
            'category': category,
            'discounted_price': discounted_price,
            'actual_price': actual_price,
            'discount_percentage': discount_percentage,
            'rating': rating,
            'rating_count': rating_count,
            'quantity': quantity,
            'customer_id': customer_id
        }
    except Exception as e:
        return None

def calculate_revenue(order):
    """Calculate total revenue for an order"""
    return order['discounted_price'] * order['quantity']

def calculate_savings(order):
    """Calculate savings from discount"""
    return (order['actual_price'] - order['discounted_price']) * order['quantity']

def get_category_group(category):
    """Group similar categories together"""
    category_lower = category.lower()
    
    # Parse the pipe-separated category path
    if '|' in category:
        main_category = category.split('|')[0].strip()
    else:
        main_category = category
    
    main_lower = main_category.lower()
    
    if 'computer' in main_lower or 'electronic' in main_lower:
        return 'Computers & Electronics'
    elif 'cloth' in main_lower or 'fashion' in main_lower:
        return 'Clothing & Fashion'
    elif 'home' in main_lower or 'kitchen' in main_lower:
        return 'Home & Kitchen'
    elif 'book' in main_lower:
        return 'Books'
    elif 'toy' in main_lower or 'game' in main_lower:
        return 'Toys & Games'
    elif 'health' in main_lower or 'beauty' in main_lower:
        return 'Health & Beauty'
    elif 'sport' in main_lower:
        return 'Sports & Fitness'
    elif 'car' in main_lower or 'automotive' in main_lower:
        return 'Automotive'
    elif 'office' in main_lower:
        return 'Office Products'
    elif 'music' in main_lower or 'instrument' in main_lower:
        return 'Musical Instruments'
    else:
        return main_category

# ==================== TRANSFORM FUNCTIONS ====================

class EnrichWithCategoryGroup(beam.DoFn):
    """Add category group to order data"""
    def process(self, order):
        if order:
            order['category_group'] = get_category_group(order['category'])
            yield order

class ExtractCategoryRevenue(beam.DoFn):
    """Extract category and revenue from order"""
    def process(self, order):
        if order:
            revenue = calculate_revenue(order)
            yield (order['category_group'], revenue)

class ExtractProductRevenue(beam.DoFn):
    """Extract product and revenue from order"""
    def process(self, order):
        if order:
            revenue = calculate_revenue(order)
            # Truncate long product names
            product_name = order['product_name'][:80]
            yield (product_name, revenue)

class ExtractCustomerValue(beam.DoFn):
    """Extract customer ID and their purchase value"""
    def process(self, order):
        if order:
            revenue = calculate_revenue(order)
            yield (order['customer_id'], revenue)

class SegmentCustomer(beam.DoFn):
    """Segment customers based on lifetime value"""
    def process(self, customer_data):
        customer_id, total_value = customer_data
        
        if total_value >= 5000:
            segment = 'VIP'
        elif total_value >= 1000:
            segment = 'Premium'
        elif total_value >= 200:
            segment = 'Regular'
        else:
            segment = 'Casual'
        
        yield {
            'customer_id': customer_id,
            'total_value': total_value,
            'segment': segment
        }

class ExtractDiscountAnalysis(beam.DoFn):
    """Analyze discount effectiveness"""
    def process(self, order):
        if order:
            discount_pct = order['discount_percentage']
            
            if discount_pct == 0:
                discount_range = 'No Discount'
            elif discount_pct < 20:
                discount_range = '1-20% OFF'
            elif discount_pct < 40:
                discount_range = '20-40% OFF'
            elif discount_pct < 60:
                discount_range = '40-60% OFF'
            else:
                discount_range = '60%+ OFF'
            
            revenue = calculate_revenue(order)
            yield (discount_range, revenue)

class ExtractRatingAnalysis(beam.DoFn):
    """Analyze products by rating"""
    def process(self, order):
        if order and order['rating'] > 0:
            rating = order['rating']
            
            if rating >= 4.5:
                rating_range = '4.5-5.0 ⭐⭐⭐⭐⭐'
            elif rating >= 4.0:
                rating_range = '4.0-4.4 ⭐⭐⭐⭐'
            elif rating >= 3.0:
                rating_range = '3.0-3.9 ⭐⭐⭐'
            else:
                rating_range = 'Below 3.0 ⭐⭐'
            
            revenue = calculate_revenue(order)
            yield (rating_range, revenue)

class FindHighValueProducts(beam.DoFn):
    """Find high-value products (price > ₹5000)"""
    def process(self, order):
        if order and order['actual_price'] > 5000:
            yield {
                'product_name': order['product_name'][:70],
                'actual_price': order['actual_price'],
                'discounted_price': order['discounted_price'],
                'discount': order['discount_percentage'],
                'rating': order['rating'],
                'rating_count': order['rating_count']
            }

class FindBestDeals(beam.DoFn):
    """Find best deals (high discount + high rating)"""
    def process(self, order):
        if order and order['discount_percentage'] >= 40 and order['rating'] >= 4.0:
            savings = order['actual_price'] - order['discounted_price']
            yield {
                'product_name': order['product_name'][:60],
                'original': order['actual_price'],
                'discounted': order['discounted_price'],
                'savings': savings,
                'discount': order['discount_percentage'],
                'rating': order['rating'],
                'rating_count': order['rating_count']
            }

class FindTopRatedProducts(beam.DoFn):
    """Find top-rated products with significant reviews"""
    def process(self, order):
        if order and order['rating'] >= 4.5 and order['rating_count'] > 1000:
            yield {
                'product_name': order['product_name'][:70],
                'rating': order['rating'],
                'rating_count': order['rating_count'],
                'price': order['discounted_price']
            }

# ==================== FORMAT FUNCTIONS ====================

class FormatTopN(beam.DoFn):
    """Generic formatter for top N results"""
    def process(self, data):
        key, value = data
        yield f"{key:40} ₹{value:>12,.2f}"

class FormatCustomerReport(beam.DoFn):
    """Format customer segmentation report"""
    def process(self, customer):
        yield f"[{customer['segment']:8}] {customer['customer_id']:30} ₹{customer['total_value']:>10,.2f}"

class FormatHighValueProduct(beam.DoFn):
    """Format high-value products"""
    def process(self, product):
        yield f"{product['product_name']:70} | MRP: ₹{product['actual_price']:>8,.0f} | Sale: ₹{product['discounted_price']:>8,.0f} | {product['discount']:>3.0f}% OFF | {product['rating']:.1f}⭐ ({product['rating_count']:,} reviews)"

class FormatBestDeal(beam.DoFn):
    """Format best deals"""
    def process(self, deal):
        yield f"💰 {deal['product_name']:60} | Save ₹{deal['savings']:>7,.0f} ({deal['discount']:.0f}% OFF) | {deal['rating']:.1f}⭐ ({deal['rating_count']:,}) | Now: ₹{deal['discounted']:,.0f}"

class FormatTopRated(beam.DoFn):
    """Format top-rated products"""
    def process(self, product):
        yield f"⭐ {product['rating']:.1f} ({product['rating_count']:>6,} reviews) | {product['product_name']:60} | ₹{product['price']:,.0f}"

class CreateSummaryStats(beam.DoFn):
    """Create summary statistics"""
    def process(self, all_orders):
        orders = list(all_orders)
        if not orders:
            yield "No valid orders found"
            return
            
        total_orders = len(orders)
        total_revenue = sum(calculate_revenue(o) for o in orders)
        total_actual_value = sum(o['actual_price'] * o['quantity'] for o in orders)
        total_savings = total_actual_value - total_revenue
        unique_customers = len(set(o['customer_id'] for o in orders))
        unique_products = len(set(o['product_id'] for o in orders))
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        avg_discount = sum(o['discount_percentage'] for o in orders) / total_orders if total_orders > 0 else 0
        
        orders_with_ratings = [o for o in orders if o['rating'] > 0]
        avg_rating = sum(o['rating'] for o in orders_with_ratings) / len(orders_with_ratings) if orders_with_ratings else 0
        total_reviews = sum(o['rating_count'] for o in orders)
        
        yield f"{'='*80}"
        yield f"AMAZON SALES ANALYTICS SUMMARY"
        yield f"{'='*80}"
        yield f"Total Products Analyzed: {unique_products:,}"
        yield f"Total Simulated Orders: {total_orders:,}"
        yield f"Total Revenue (After Discount): ₹{total_revenue:,.2f}"
        yield f"Total MRP Value: ₹{total_actual_value:,.2f}"
        yield f"Total Customer Savings: ₹{total_savings:,.2f} ({(total_savings/total_actual_value)*100:.1f}%)"
        yield f"Unique Customers: {unique_customers:,}"
        yield f"Average Order Value: ₹{avg_order_value:,.2f}"
        yield f"Average Discount: {avg_discount:.1f}%"
        yield f"Average Product Rating: {avg_rating:.2f}⭐"
        yield f"Total Customer Reviews: {total_reviews:,}"
        yield f"{'='*80}"

# ==================== MAIN PIPELINE ====================

def run_pipeline():
    """Execute the Amazon sales analytics pipeline"""
    
    # Create output directory
    os.makedirs('outputs', exist_ok=True)
    
    print("🚀 Starting Amazon Sales Analytics Pipeline...")
    print("📊 Processing Real Amazon Dataset from Kaggle")
    print("=" * 70)
    
    # Check if data file exists
    data_file = 'data/amazon.csv'
    if not os.path.exists(data_file):
        data_file = 'data/sales_data.csv'
        if not os.path.exists(data_file):
            print("ERROR: Dataset not found!")
            print("Please place your Amazon CSV as:")
            print("   data/amazon.csv  OR  data/sales_data.csv")
            return
    
    print(f"✓ Found dataset: {data_file}")
    
    # Pipeline options
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as pipeline:
        
        # ===== READ AND CLEAN DATA =====
        orders = (
            pipeline
            | 'Read Sales Data' >> beam.io.ReadFromText(data_file, skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(parse_csv_line)
            | 'Filter Valid Orders' >> beam.Filter(lambda x: x is not None)
            | 'Add Category Group' >> beam.ParDo(EnrichWithCategoryGroup())
        )
        
        # ===== SUMMARY STATISTICS =====
        summary = (
            orders
            | 'Collect All Orders' >> beam.combiners.ToList()
            | 'Create Summary' >> beam.ParDo(CreateSummaryStats())
            | 'Write Summary' >> beam.io.WriteToText(
                'outputs/00_summary',
                shard_name_template='',
                file_name_suffix='.txt'
            )
        )
        
        # ===== ANALYSIS 1: Revenue by Category =====
        category_revenue = (
            orders
            | 'Extract Category Revenue' >> beam.ParDo(ExtractCategoryRevenue())
            | 'Sum by Category' >> beam.CombinePerKey(sum)
            | 'Sort Categories' >> beam.transforms.combiners.Top.Of(15, key=lambda x: x[1])
            | 'Flatten Category List' >> beam.FlatMap(lambda x: x)
            | 'Format Category Report' >> beam.ParDo(FormatTopN())
            | 'Write Category Report' >> beam.io.WriteToText(
                'outputs/01_revenue_by_category',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== REVENUE BY PRODUCT CATEGORY ==='
            )
        )
        
        # ===== ANALYSIS 2: Top Products by Revenue =====
        top_products = (
            orders
            | 'Extract Product Revenue' >> beam.ParDo(ExtractProductRevenue())
            | 'Sum by Product' >> beam.CombinePerKey(sum)
            | 'Sort Products' >> beam.transforms.combiners.Top.Of(30, key=lambda x: x[1])
            | 'Flatten Product List' >> beam.FlatMap(lambda x: x)
            | 'Format Product Report' >> beam.ParDo(FormatTopN())
            | 'Write Product Report' >> beam.io.WriteToText(
                'outputs/02_top_products',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== TOP 30 PRODUCTS BY REVENUE ==='
            )
        )
        
        # ===== ANALYSIS 3: Customer Segmentation =====
        customer_segments = (
            orders
            | 'Extract Customer Value' >> beam.ParDo(ExtractCustomerValue())
            | 'Sum Customer Value' >> beam.CombinePerKey(sum)
            | 'Segment Customers' >> beam.ParDo(SegmentCustomer())
            | 'Format Customer Report' >> beam.ParDo(FormatCustomerReport())
            | 'Write Customer Report' >> beam.io.WriteToText(
                'outputs/03_customer_segments',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== CUSTOMER SEGMENTATION (by Lifetime Value) ==='
            )
        )
        
        # ===== ANALYSIS 4: Discount Effectiveness =====
        discount_analysis = (
            orders
            | 'Extract Discount Analysis' >> beam.ParDo(ExtractDiscountAnalysis())
            | 'Sum by Discount Range' >> beam.CombinePerKey(sum)
            | 'Format Discount Report' >> beam.ParDo(FormatTopN())
            | 'Write Discount Report' >> beam.io.WriteToText(
                'outputs/04_discount_analysis',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== REVENUE BY DISCOUNT RANGE ==='
            )
        )
        
        # ===== ANALYSIS 5: Rating Analysis =====
        rating_analysis = (
            orders
            | 'Extract Rating Analysis' >> beam.ParDo(ExtractRatingAnalysis())
            | 'Sum by Rating Range' >> beam.CombinePerKey(sum)
            | 'Format Rating Report' >> beam.ParDo(FormatTopN())
            | 'Write Rating Report' >> beam.io.WriteToText(
                'outputs/05_rating_analysis',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== REVENUE BY PRODUCT RATING ==='
            )
        )
        
        # ===== ANALYSIS 6: High-Value Products =====
        high_value = (
            orders
            | 'Find High Value Products' >> beam.ParDo(FindHighValueProducts())
            | 'Sort by Price' >> beam.transforms.combiners.Top.Of(25, key=lambda x: x['actual_price'])
            | 'Flatten High Value List' >> beam.FlatMap(lambda x: x)
            | 'Format High Value Report' >> beam.ParDo(FormatHighValueProduct())
            | 'Write High Value Report' >> beam.io.WriteToText(
                'outputs/06_high_value_products',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== TOP 25 PREMIUM PRODUCTS (MRP > ₹5,000) ==='
            )
        )
        
        # ===== ANALYSIS 7: Best Deals =====
        best_deals = (
            orders
            | 'Find Best Deals' >> beam.ParDo(FindBestDeals())
            | 'Sort Best Deals' >> beam.transforms.combiners.Top.Of(40, key=lambda x: x['discount'])
            | 'Flatten Deals List' >> beam.FlatMap(lambda x: x)
            | 'Format Best Deals' >> beam.ParDo(FormatBestDeal())
            | 'Write Best Deals' >> beam.io.WriteToText(
                'outputs/07_best_deals',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== TOP 40 BEST DEALS (40%+ OFF & 4+ Stars) ==='
            )
        )
        
        # ===== ANALYSIS 8: Top Rated Products =====
        top_rated = (
            orders
            | 'Find Top Rated' >> beam.ParDo(FindTopRatedProducts())
            | 'Sort by Rating Count' >> beam.transforms.combiners.Top.Of(30, key=lambda x: x['rating_count'])
            | 'Flatten Top Rated List' >> beam.FlatMap(lambda x: x)
            | 'Format Top Rated' >> beam.ParDo(FormatTopRated())
            | 'Write Top Rated' >> beam.io.WriteToText(
                'outputs/08_top_rated_products',
                shard_name_template='',
                file_name_suffix='.txt',
                header='=== TOP 30 HIGHLY RATED PRODUCTS (4.5+ ⭐ with 1000+ reviews) ==='
            )
        )
    
    print("\n✅ Pipeline execution complete!")
    print("\n📁 Generated Analytics Reports:")
    print("   outputs/00_summary.txt - Overall statistics")
    print("   outputs/01_revenue_by_category.txt - Revenue by category")
    print("   outputs/02_top_products.txt - Best-selling products")
    print("   outputs/03_customer_segments.txt - Customer segmentation")
    print("   outputs/04_discount_analysis.txt - Discount effectiveness")
    print("   outputs/05_rating_analysis.txt - Revenue by rating")
    print("   outputs/06_high_value_products.txt - Premium products")
    print("   outputs/07_best_deals.txt - Best deals for customers")
    print("   outputs/08_top_rated_products.txt - Highly-rated products")
    print("\n" + "=" * 70)
    print("💡 View reports: cat outputs/00_summary.txt")

if __name__ == '__main__':
    run_pipeline()