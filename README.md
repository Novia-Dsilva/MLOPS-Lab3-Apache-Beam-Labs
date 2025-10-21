# MLOPS-Lab3-Apache-Beam-Labs

# Amazon E-Commerce Analytics using Apache Beam

A comprehensive data processing pipeline that analyzes Amazon product data using Apache Beam, with a FastAPI backend and interactive web dashboard.

---

## Project Overview

This project demonstrates the transformation of Apache Beam's classic **word count example** into a real-world **e-commerce analytics system**. We process Amazon product data to generate business insights including revenue analysis, customer segmentation, and product recommendations.

---

## What We Built

### **Core Pipeline: Word Count on Amazon Products**
- **Original**: Count word frequencies in Shakespeare's King Lear text file
- **Modified**: Count word frequencies in Amazon product names (CSV data)
- **Advancement**: Added CSV parsing, data validation, and dual output streams

### **Extended Analytics: 8 Business Reports**
1. **Summary Statistics** - Overall sales metrics
2. **Revenue by Category** - Top performing product categories
3. **Top Products** - Best-selling items by revenue
4. **Customer Segmentation** - VIP, Premium, Regular, Casual tiers
5. **Discount Analysis** - Revenue by discount ranges
6. **Rating Analysis** - Product quality distribution
7. **High-Value Products** - Premium items (>₹5,000)
8. **Best Deals** - High discount + high rating combinations

### **Web Dashboard & API**
- **FastAPI Backend**: REST API to trigger pipelines and fetch results
- **Interactive Dashboard**: Real-time analytics visualization with Chart.js
- **Run Pipeline Button**: Execute Apache Beam pipeline from web interface

---

## 🔄 Key Modifications from Original Tutorial

| Component | Original | Our Implementation |
|-----------|----------|-------------------|
| **Input** | Text file (`kinglear.txt`) | CSV file (`amazon.csv`) |
| **Data Source** | Plain text lines | Structured e-commerce data |
| **Parsing** | None needed | CSV reader with field extraction |
| **Data Cleaning** | None | Price/rating/percentage cleaning |
| **Validation** | None | Filter null/invalid entries |
| **Outputs** | Single word count | 8 analytics reports + word count |
| **Backend** | None | FastAPI REST API |
| **Frontend** | None | Interactive HTML dashboard |

---

## 🛠️ Technical Stack

- **Apache Beam** - Data processing pipeline framework
- **Python 3.x** - Core programming language
- **FastAPI** - REST API backend
- **Chart.js** - Data visualization
- **HTML/CSS/JavaScript** - Interactive dashboard

---

## 📁 Project Structure

```
Apache_Beam_Amazon_Analytics/
├── data/
│   └── amazon.csv                          # Input dataset (1,351 products)
│
├── outputs/                                 # Generated analytics reports
│   ├── 00_summary.txt                      # Overall statistics
│   ├── 01_revenue_by_category.txt          # Category performance
│   ├── 02_top_products.txt                 # Best sellers
│   ├── 03_customer_segments.txt            # Customer tiers
│   ├── 04_discount_analysis.txt            # Discount effectiveness
│   ├── 05_rating_analysis.txt              # Quality metrics
│   ├── 06_high_value_products.txt          # Premium products
│   ├── 07_best_deals.txt                   # Best offers
│   ├── 08_top_rated_products.txt           # Top quality items
│   ├── amazon_word_count-00000-of-00001    # Word frequency (raw)
│   └── amazon_top_50_words.txt             # Top 50 words (formatted)
│
├── pipeline.py                              # Main Apache Beam pipeline
├── api.py                                   # FastAPI backend server
├── dashboard.html                           # Web visualization interface
├── Try_Apache_Beam_Python.ipynb            # Jupyter notebook
└── README.md                                # This file
```

---

## 🚀 How to Run

### **1. Install Dependencies**
```bash
pip install apache-beam fastapi uvicorn
```

### **2. Run Apache Beam Pipeline**
```bash
python pipeline.py
```

### **3. Start FastAPI Backend**
```bash
python api.py
# API available at: http://localhost:8000
# API Docs: http://localhost:8000/docs
```

### **4. Open Dashboard**
```bash
# Open dashboard.html in your browser
# Or use a local server:
python -m http.server 8080
# Then visit: http://localhost:8080/dashboard.html
```

### **5. Run in Jupyter Notebook**
```python
# Open Try_Apache_Beam_Python.ipynb
# Execute cells to run word count analysis
```

---

## 📊 Sample Outputs

### **Word Count Analysis**
```
=== TOP 50 MOST COMMON WORDS IN AMAZON PRODUCT NAMES ===
with                      752 times
for                       674 times
black                     530 times
usb                       419 times
cable                     414 times
charging                  247 times
```

### **Summary Statistics**
```
================================================================================
AMAZON SALES ANALYTICS SUMMARY
================================================================================
Total Products Analyzed: 1,351
Total Simulated Orders: 1,465
Total Revenue (After Discount): ₹6,471,737.43
Total Customer Savings: ₹4,916,692.81 (43.2%)
Average Product Rating: 4.10⭐
================================================================================
```

### **Top Categories by Revenue**
```
Computers & Electronics                  ₹4,808,111.50
Home & Kitchen                           ₹1,640,767.93
Office Products                          ₹   13,360.00
```

### **Best Deals**
```
💰 Fire-Boltt Ninja Call Pro Plus 1.83" Smart Watch | Save ₹18,200 (91% OFF) | 4.2⭐ | Now: ₹1,799
💰 rts [2 Pack] Mini USB C Type C Adapter | Save ₹4,705 (94% OFF) | 4.3⭐ | Now: ₹294
```

---

## 🎓 Apache Beam Concepts Demonstrated

| Concept | Usage |
|---------|-------|
| **ReadFromText** | Read CSV files line by line |
| **Map** | Parse CSV, transform data |
| **ParDo** | Custom transformations (DoFn classes) |
| **Filter** | Remove invalid entries |
| **FlatMap** | Extract multiple elements |
| **CombinePerKey** | Aggregate data by key |
| **GroupByKey** | Group values by key |
| **Top.Of()** | Get top N results |
| **WriteToText** | Save results to files |

---

## 💡 Key Features

### **1. CSV Data Handling**
- Robust parsing of quoted fields with commas
- Price cleaning (₹ symbol, comma removal)
- Percentage and rating normalization
- Null value filtering

### **2. Business Analytics**
- Customer lifetime value segmentation
- Discount effectiveness analysis
- Product quality correlation with revenue
- Best deal identification (discount + rating)

### **3. Web Integration**
- RESTful API endpoints for all analytics
- Real-time dashboard with interactive charts
- One-click pipeline execution from browser
- Automatic data refresh

### **4. Scalable Architecture**
- DirectRunner for local testing
- Ready for DataflowRunner (Google Cloud)
- Modular DoFn classes for transformations
- Reusable formatting functions

---

## 📸 Screenshots

### **Dashboard Overview**
![Dashboard](screenshots/dashboard.png)
*Interactive analytics dashboard showing KPIs, charts, and best deals*

### **Pipeline Execution**
![Pipeline Output](screenshots/pipeline_output.png)
*Apache Beam pipeline generating 8 analytics reports*

### **Word Count Results**
![Word Count](screenshots/word_count.png)
*Top 50 most common words in Amazon product names*

### **FastAPI Documentation**
![API Docs](screenshots/api_docs.png)
*Auto-generated API documentation at /docs endpoint*

---

## 🔍 API Endpoints

```
GET  /health                              # System health check
GET  /analytics/summary                   # Overall statistics
GET  /analytics/categories                # Revenue by category
GET  /analytics/products?limit=20         # Top products
GET  /analytics/customers?limit=50        # Customer segments
GET  /analytics/deals?limit=20            # Best deals
GET  /analytics/customer-segments-summary # Segment distribution
POST /run-pipeline                        # Trigger pipeline execution
GET  /download/{report_name}              # Download specific report
```


## 🎯 Learning Outcomes

Adapted text processing pipeline to structured CSV data  
Implemented robust data parsing and validation  
Created multiple parallel analysis streams  
Built REST API for data access  
Developed interactive visualization dashboard  
Demonstrated Apache Beam's flexibility and scalability  



---
**Novia D'Silva**  
MLOps Lab Assignment - Apache Beam Analytics

---
