# PPC Optimizer

Amazon Ads PPC optimization tool for analyzing search term reports, identifying harvest opportunities, generating negative keywords, and optimizing bids.

## Quick Start

### 1. Upload Files

When you first open the app, you'll see a landing page with three file upload sections:

#### ✅ Main Report (Required)
- **File:** Search Term Report
- **Source:** Amazon Ads Console → Reports → Search Term Report
- **Required:** Yes

#### 🆔 ID Mapping (Optional)
- **File:** Bulk Upload Template (Sponsored Products Campaigns)
- **Source:** Amazon Ads Console → Bulk Operations → Download
- **Used for:** Bid Updates with Keyword/Product Targeting IDs
- **Recommended:** Yes, for bid optimization

#### 🏷️ SKU Mapping (Optional)
- **File:** Purchased Product Report
- **Source:** Amazon Ads Console → Reports → Purchased Product Report
- **Used for:** Harvest Campaigns (prevents SKU_NEEDED errors)
- **Recommended:** Yes, if creating new campaigns

### 2. Analyze & Optimize

After uploading, you'll have access to 8 analysis tabs:

- **📊 Overview** - Performance summary and analysis report
- **💎 Harvest** - High-performing search terms to promote to exact match
- **🛑 Negatives** - Poor performers to add as negative keywords
- **💰 Bids** - Optimize bids for existing keywords/targets
- **⚠️ Cannibalization** - Detect internal competition
- **📈 Velocity** - Track keyword performance trends
- **🎯 Simulation** - Forecast impact of changes
- **🚀 Actions** - Export bulk files for upload

### 3. Download & Upload

Each tab (Negatives, Bids, Actions) provides:
- **Auto-validation** against Amazon requirements
- **✅ Valid Rows** - Ready for Amazon upload
- **❌ Error Rows** - Download to fix manually
- **Clear error messages** with row numbers

## Features

### ✅ Automatic Validation
- Validates all bulk files against Amazon Advertising requirements
- Checks UPDATE operations for required IDs
- Validates Product Targeting vs Keyword requirements
- Flags SKU_NEEDED placeholders
- Ensures numeric Campaign/Ad Group IDs

### 📊 Smart Analysis
- ROAS-based bid optimization
- Harvest candidate identification
- Negative keyword detection
- Cannibalization analysis
- Velocity tracking

### 🚀 Bulk File Generation
- Harvest campaigns (new exact match)
- Negative keywords/targets
- Bid updates (existing keywords/targets)
- All files validated before download

## Tab Guide

### Overview Tab
- View high-level metrics
- Download analysis report

### Harvest Tab
- Identify high-performing search terms
- Prepare for campaign creation
- Send to Actions tab

### Negatives Tab
- Identify poor-performing terms
- Generate negative keywords bulk file
- Auto-validated with error separation

### Bids Tab
- Optimize bids based on ROAS
- Direct (Exact/PT) and Aggregated (Broad/Phrase/Auto)
- Requires ID Mapping for full functionality
- Auto-validated with error separation

### Cannibalization Tab
- Detect same ASIN/keyword in multiple campaigns
- View wasted spend
- Estimate savings potential

### Velocity Tab
- Track performance trends over time
- Requires 2+ uploads
- Rising/Falling/Stable indicators

### Simulation Tab
- Forecast impact of bid changes
- Projected spend and sales
- Risk analysis

### Actions Tab
- Generate harvest campaign bulk files
- Requires SKU Mapping for automatic SKU assignment
- Auto-validated with error separation

## Data Files Reference

### Main Upload (Required)
**Search Term Report**
- Source: Amazon Ads Console → Reports → Search Term Report
- Frequency: Upload whenever you want fresh analysis
- Contains: Campaign data, keywords, performance metrics

### Optional Mapping Files

**1. ID Mapping (for Bids Tab)**
- Source: Amazon Ads Console → Bulk Operations → Download → Sponsored Products Campaigns
- Frequency: Download fresh before each bid update
- Contains: Keyword ID, Product Targeting ID, Campaign ID, Ad Group ID
- **Critical for bid updates to work!**

**2. SKU Mapping (for Actions Tab)**
- Source: Amazon Ads Console → Reports → Purchased Product Report
- Frequency: Download when creating new harvest campaigns
- Contains: Advertised SKU, ASIN
- **Prevents SKU_NEEDED errors in harvest campaigns**

## Typical Workflow

### Weekly Optimization Cycle

1. **Download from Amazon:**
   - Search Term Report
   - Bulk upload template (for IDs)
   - Purchased Product Report (if creating campaigns)

2. **Upload to Optimizer:**
   - Main: Search Term Report
   - Optional: ID mapping file
   - Optional: SKU mapping file

3. **Generate & Download:**
   - Negatives → Download valid rows → Upload to Amazon
   - Bids → Download valid rows → Upload to Amazon
   - Harvest → Download valid rows → Upload to Amazon

4. **Fix Errors (if any):**
   - Download error rows
   - Fix manually in Excel
   - Re-upload to Amazon

## Validation System

All tabs auto-validate bulk files before download:

### What Gets Validated
- UPDATE operations have required IDs
- Product Targeting has expressions (not keyword text)
- Keywords have text and match types
- SKUs are not placeholders (SKU_NEEDED)
- Campaign/Ad Group IDs are numeric

### Validation Output
- **✅ Valid Rows** - Ready for Amazon upload
- **❌ Error Rows** - Download to fix manually
- **Error details** - Specific row numbers and issues

## Pro Tips

1. **Always download fresh ID mapping** before bid updates
2. **Use Purchased Product Report** to avoid SKU_NEEDED errors
3. **Upload Search Term Report regularly** for accurate trends
4. **Fix errors before uploading** to Amazon (or they'll be rejected)
5. **Download valid rows only** for fastest uploads
6. **Keep mapping files updated** to avoid validation errors

## Performance

- Optimized for large datasets (10,000+ rows)
- Fast validation with helper functions
- Minimal code duplication
- Efficient error extraction

## Support

For issues or questions:
- Check the Tab Guide (📖 button in app)
- Review error messages (they're specific!)
- Ensure mapping files are up to date

---

**Ready to optimize your PPC campaigns!** 🚀
