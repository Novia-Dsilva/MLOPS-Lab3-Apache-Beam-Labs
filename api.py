"""
FastAPI Backend for E-Commerce Analytics Pipeline
Provides REST API endpoints to trigger pipeline and fetch results
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import subprocess
import json
import os
from datetime import datetime
from typing import List, Dict, Optional

app = FastAPI(
    title="E-Commerce Analytics API",
    description="API for Amazon Sales Analytics using Apache Beam",
    version="1.0.0"
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== MODELS ====================

class PipelineStatus(BaseModel):
    status: str
    message: str
    timestamp: str

class SummaryStats(BaseModel):
    total_products: int
    total_orders: int
    total_revenue: float
    total_savings: float
    avg_order_value: float
    avg_discount: float
    avg_rating: float

class CategoryRevenue(BaseModel):
    category: str
    revenue: float

class ProductRevenue(BaseModel):
    product: str
    revenue: float

class CustomerSegment(BaseModel):
    segment: str
    customer_id: str
    total_value: float

class BestDeal(BaseModel):
    product: str
    savings: float
    discount: float
    rating: float
    current_price: float

# ==================== HELPER FUNCTIONS ====================

def parse_summary_file():
    """Parse summary statistics from output file"""
    try:
        with open('outputs/00_summary.txt', 'r', encoding='utf-8') as f:
            content = f.read()
        
        lines = content.strip().split('\n')
        stats = {}
        
        for line in lines:
            if 'Total Products Analyzed:' in line:
                stats['total_products'] = int(line.split(':')[1].strip().replace(',', ''))
            elif 'Total Simulated Orders:' in line:
                stats['total_orders'] = int(line.split(':')[1].strip().replace(',', ''))
            elif 'Total Revenue (After Discount):' in line:
                revenue_str = line.split('₹')[1].strip().replace(',', '')
                stats['total_revenue'] = float(revenue_str)
            elif 'Total Customer Savings:' in line:
                savings_part = line.split('₹')[1].split('(')[0].strip().replace(',', '')
                stats['total_savings'] = float(savings_part)
            elif 'Average Order Value:' in line:
                avg_str = line.split('₹')[1].strip().replace(',', '')
                stats['avg_order_value'] = float(avg_str)
            elif 'Average Discount:' in line:
                discount_str = line.split(':')[1].strip().replace('%', '')
                stats['avg_discount'] = float(discount_str)
            elif 'Average Product Rating:' in line:
                rating_str = line.split(':')[1].strip().replace('⭐', '')
                stats['avg_rating'] = float(rating_str)
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing summary: {str(e)}")

def parse_category_file():
    """Parse category revenue from output file"""
    try:
        categories = []
        with open('outputs/01_revenue_by_category.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            if line.startswith('===') or not line.strip():
                continue
            
            parts = line.strip().split('₹')
            if len(parts) == 2:
                category = parts[0].strip()
                revenue = float(parts[1].strip().replace(',', ''))
                categories.append({'category': category, 'revenue': revenue})
        
        return categories
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing categories: {str(e)}")

def parse_products_file():
    """Parse top products from output file"""
    try:
        products = []
        with open('outputs/02_top_products.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            if line.startswith('===') or not line.strip():
                continue
            
            parts = line.strip().split('₹')
            if len(parts) == 2:
                product = parts[0].strip()[:50]  # Truncate long names
                revenue = float(parts[1].strip().replace(',', ''))
                products.append({'product': product, 'revenue': revenue})
        
        return products
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing products: {str(e)}")

def parse_customers_file():
    """Parse customer segments from output file"""
    try:
        customers = []
        with open('outputs/03_customer_segments.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            if line.startswith('===') or not line.strip():
                continue
            
            # Format: [Segment] CustomerID ₹Value
            if '₹' in line:
                parts = line.strip().split('₹')
                left_part = parts[0]
                value = float(parts[1].strip().replace(',', ''))
                
                # Extract segment and customer_id
                bracket_parts = left_part.split(']')
                if len(bracket_parts) == 2:
                    segment = bracket_parts[0].replace('[', '').strip()
                    customer_id = bracket_parts[1].strip()
                    
                    customers.append({
                        'segment': segment,
                        'customer_id': customer_id,
                        'total_value': value
                    })
        
        return customers
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing customers: {str(e)}")

def parse_deals_file():
    """Parse best deals from output file"""
    try:
        deals = []
        with open('outputs/07_best_deals.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            if line.startswith('===') or not line.strip() or line.startswith('TOP'):
                continue
            
            # Format: 💰 Product | Save ₹X (Y% OFF) | Z⭐ | Now: ₹Price
            if '💰' in line and '₹' in line and '|' in line:
                try:
                    parts = line.split('|')
                    if len(parts) < 4:
                        continue
                    
                    product = parts[0].replace('💰', '').strip()[:50]
                    
                    # Extract savings
                    savings_part = parts[1].strip()
                    if 'Save ₹' in savings_part:
                        savings_str = savings_part.split('Save ₹')[1].split('(')[0].strip().replace(',', '')
                        savings = float(savings_str)
                    else:
                        continue
                    
                    # Extract discount percentage
                    if '% OFF' in savings_part:
                        discount_str = savings_part.split('(')[1].split('%')[0].strip()
                        discount = float(discount_str)
                    else:
                        continue
                    
                    # Extract rating
                    rating_part = parts[2].strip()
                    if '⭐' in rating_part:
                        rating_str = rating_part.split('⭐')[0].strip()
                        rating = float(rating_str)
                    else:
                        continue
                    
                    # Extract current price
                    price_part = parts[3].strip()
                    if 'Now: ₹' in price_part:
                        price_str = price_part.split('₹')[1].strip().replace(',', '')
                        current_price = float(price_str)
                    else:
                        continue
                    
                    deals.append({
                        'product': product,
                        'savings': savings,
                        'discount': discount,
                        'rating': rating,
                        'current_price': current_price
                    })
                except (ValueError, IndexError) as e:
                    # Skip malformed lines
                    continue
        
        return deals
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Deals file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing deals: {str(e)}")
    
def run_pipeline_background():
    """Run the Apache Beam pipeline in background"""
    try:
        result = subprocess.run(
            ['python', 'pipeline.py'],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        return result.returncode == 0
    except Exception as e:
        print(f"Pipeline error: {str(e)}")
        return False

# ==================== API ENDPOINTS ====================

@app.get("/")
def root():
    """Root endpoint"""
    return {
        "message": "E-Commerce Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "summary": "/analytics/summary",
            "categories": "/analytics/categories",
            "products": "/analytics/products",
            "customers": "/analytics/customers",
            "deals": "/analytics/deals",
            "run_pipeline": "/run-pipeline"
        }
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    outputs_exist = os.path.exists('outputs/00_summary.txt')
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "outputs_available": outputs_exist
    }

@app.post("/run-pipeline")
async def run_pipeline(background_tasks: BackgroundTasks):
    """Trigger the Apache Beam pipeline"""
    
    # Check if pipeline.py exists
    if not os.path.exists('pipeline.py'):
        raise HTTPException(status_code=404, detail="pipeline.py not found")
    
    # Run pipeline in background
    background_tasks.add_task(run_pipeline_background)
    
    return PipelineStatus(
        status="started",
        message="Pipeline execution started in background",
        timestamp=datetime.now().isoformat()
    )

@app.get("/analytics/summary", response_model=SummaryStats)
def get_summary():
    """Get summary statistics"""
    
    if not os.path.exists('outputs/00_summary.txt'):
        raise HTTPException(status_code=404, detail="Summary file not found. Run pipeline first.")
    
    stats = parse_summary_file()
    
    return SummaryStats(
        total_products=stats.get('total_products', 0),
        total_orders=stats.get('total_orders', 0),
        total_revenue=stats.get('total_revenue', 0.0),
        total_savings=stats.get('total_savings', 0.0),
        avg_order_value=stats.get('avg_order_value', 0.0),
        avg_discount=stats.get('avg_discount', 0.0),
        avg_rating=stats.get('avg_rating', 0.0)
    )

@app.get("/analytics/categories", response_model=List[CategoryRevenue])
def get_categories():
    """Get revenue by category"""
    
    if not os.path.exists('outputs/01_revenue_by_category.txt'):
        raise HTTPException(status_code=404, detail="Category file not found")
    
    categories = parse_category_file()
    return [CategoryRevenue(**cat) for cat in categories]

@app.get("/analytics/products", response_model=List[ProductRevenue])
def get_products(limit: int = 20):
    """Get top products by revenue"""
    
    if not os.path.exists('outputs/02_top_products.txt'):
        raise HTTPException(status_code=404, detail="Products file not found")
    
    products = parse_products_file()
    return [ProductRevenue(**prod) for prod in products[:limit]]

@app.get("/analytics/customers", response_model=List[CustomerSegment])
def get_customers(limit: int = 50):
    """Get customer segmentation"""
    
    if not os.path.exists('outputs/03_customer_segments.txt'):
        raise HTTPException(status_code=404, detail="Customers file not found")
    
    customers = parse_customers_file()
    return [CustomerSegment(**cust) for cust in customers[:limit]]

@app.get("/analytics/deals", response_model=List[BestDeal])
def get_deals(limit: int = 20):
    """Get best deals"""
    
    if not os.path.exists('outputs/07_best_deals.txt'):
        raise HTTPException(status_code=404, detail="Deals file not found")
    
    deals = parse_deals_file()
    return [BestDeal(**deal) for deal in deals[:limit]]

@app.get("/analytics/customer-segments-summary")
def get_customer_segments_summary():
    """Get customer segment distribution"""
    
    if not os.path.exists('outputs/03_customer_segments.txt'):
        raise HTTPException(status_code=404, detail="Customers file not found")
    
    customers = parse_customers_file()
    
    # Count by segment
    segment_counts = {}
    segment_revenue = {}
    
    for cust in customers:
        segment = cust['segment']
        segment_counts[segment] = segment_counts.get(segment, 0) + 1
        segment_revenue[segment] = segment_revenue.get(segment, 0) + cust['total_value']
    
    return {
        "segments": [
            {
                "segment": seg,
                "count": segment_counts[seg],
                "total_revenue": segment_revenue[seg]
            }
            for seg in ['VIP', 'Premium', 'Regular', 'Casual']
            if seg in segment_counts
        ]
    }

@app.get("/download/{report_name}")
def download_report(report_name: str):
    """Download a specific report file"""
    
    file_path = f"outputs/{report_name}.txt"
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"Report {report_name} not found")
    
    return FileResponse(
        file_path,
        media_type="text/plain",
        filename=f"{report_name}.txt"
    )

if __name__ == "__main__":
    import uvicorn
    print("🚀 Starting E-Commerce Analytics API...")
    print("📊 API Documentation: http://localhost:8000/docs")
    print("🔗 API Root: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)