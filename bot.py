import os
import asyncio
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram.error import BadRequest
from telethon import TelegramClient
from telethon.errors import ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError, FloodWaitError, RPCError, ChatForwardsRestrictedError
import threading
import glob
import re
import pandas as pd
import platform
import sqlite3
from telethon.sessions import SQLiteSession
from flask import Flask
import threading
import boto3
import io
import tempfile
import requests
import time
from telegram import Bot
from telegram.error import BadRequest, Conflict  
app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

# Run Flask in a separate thread
def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# === 🔐 Load environment variables ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
API_ID = 24916488
API_HASH = "3b7788498c56da1a02e904ff8e92d494"
FORWARD_CHANNEL = os.getenv("FORWARD_CHANNEL")
ADMIN_CODE = os.getenv("ADMIN_CODE")

# === 🔐 AWS S3 Configuration ===
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "your-telegram-bot-bucket")



# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# === 📁 File names (S3 only) ===
FORWARDED_FILE = "forwarded_messages.json"
SCRAPED_DATA_FILE = "scraped_data.json"

# === 🤖 AI Enhancement with Hugging Face API ===
HF_API_TOKEN = os.getenv("HF_API_TOKEN")
HF_API_URL = "https://api-inference.huggingface.co/models"

def query_huggingface_api(payload, model_name, max_retries=3):
    """Generic function to query Hugging Face API"""
    headers = {"Authorization": f"Bearer {HF_API_TOKEN}"}
    API_URL = f"{HF_API_URL}/{model_name}"
    
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 503:
                # Model is loading, wait and retry
                wait_time = 10 * (attempt + 1)
                print(f"⏳ Model loading, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ API Error {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"⏰ Request timeout (attempt {attempt + 1})")
            continue
        except Exception as e:
            print(f"❌ Request error: {e}")
            return None
    
    return None

def ai_classify_text(text, categories):
    """Classify text using Hugging Face zero-shot classification with better candidate labels"""
    # Use more specific and relevant candidate labels
    candidate_labels = [cat["name"] for cat in categories]
    
    # Add common product types that appear in your data
    extended_labels = candidate_labels + [
        "Urgent", "Vehicle", "Car", "Real Estate", "Property", 
        "Job", "Employment", "Service", "Business", "Electronics"
    ]
    
    payload = {
        "inputs": text,
        "parameters": {"candidate_labels": extended_labels}
    }
    
    result = query_huggingface_api(payload, "facebook/bart-large-mnli")
    
    if result and "labels" in result and "scores" in result:
        return {
            "labels": result["labels"],
            "scores": result["scores"]
        }
    return None

def ai_summarize_text(text):
    """Summarize text using Hugging Face summarization with better parameters"""
    if len(text) < 50:
        return text
    
    payload = {
        "inputs": text,
        "parameters": {
            "max_length": 60,  # Increased for better summaries
            "min_length": 20,
            "do_sample": False
        }
    }
    
    result = query_huggingface_api(payload, "facebook/bart-large-cnn")
    
    if result and isinstance(result, list) and len(result) > 0:
        summary = result[0].get("summary_text", text[:80])
        # Clean up the summary
        summary = re.sub(r'\s+', ' ', summary).strip()
        return summary
    return text[:80]

def extract_keywords_with_regex(text):
    """Enhanced keyword extraction using regex patterns"""
    text = clean_text(text).lower()
    
    # Enhanced product-related patterns
    patterns = {
        'Electronics': r'\b(phone|smartphone|laptop|tablet|computer|tv|television|headphone|earphone|camera|watch|macbook|iphone|samsung)\b',
        'Fashion': r'\b(shirt|dress|jeans|shoe|sneaker|bag|jacket|coat|accessory|jewelry|clothing|wear)\b',
        'Home Goods': r'\b(furniture|sofa|chair|table|bed|mattress|decor|kitchen|appliance|house|apartment)\b',
        'Beauty': r'\b(cosmetic|makeup|skincare|perfume|fragrance|cream|lotion|shampoo|beauty|glam)\b',
        'Sports': r'\b(sport|football|basketball|tennis|gym|fitness|equipment|gear|exercise|training)\b',
        'Vehicles': r'\b(car|bike|motorcycle|vehicle|auto|automobile|toyota|honda|bmw|mercedes|ford|chevrolet)\b',
        'Books': r'\b(book|novel|literature|magazine|textbook|reading|author|story)\b',
        'Groceries': r'\b(food|grocery|fruit|vegetable|meat|drink|beverage|rice|pasta|bread)\b',
        'Real Estate': r'\b(house|apartment|property|land|rent|sale|villa|condo|building|realestate)\b',
        'Jobs': r'\b(job|employment|work|career|vacancy|position|hire|recruitment|opportunity)\b',
        'Services': r'\b(service|repair|maintenance|cleaning|delivery|transport|consultation)\b'
    }
    
    for category, pattern in patterns.items():
        if re.search(pattern, text):
            return category
    
    # Check for urgency indicators
    if re.search(r'\burgent\b|\brush\b|\bemergency\b|\bquick\b|\bfast\b', text, re.IGNORECASE):
        return "Urgent"
    
    # Enhanced noun extraction
    words = re.findall(r'\b[a-z]{4,}\b', text)
    common_nouns = [word for word in words if word not in [
        'item', 'product', 'thing', 'restocked', 'detail', 'catch', 'new', 'sale', 
        'price', 'contact', 'sell', 'buy', 'market', 'telegram', 'channel'
    ]]
    
    return common_nouns[0].capitalize() if common_nouns else "Other"

def propose_new_category(text, classification_results, existing_categories):
    """Enhanced category proposal using API classification and keyword extraction"""
    text_lower = text.lower()
    
    # Check for urgency first
    if any(word in text_lower for word in ['urgent', 'rush', 'emergency', 'quick sale']):
        return "Urgent"
    
    if classification_results and classification_results["labels"]:
        top_category = classification_results["labels"][0]
        top_score = classification_results["scores"][0]
        
        # If confidence is high, use the classified category
        if top_score > 0.6:  # Lowered threshold for better categorization
            return top_category
    
    # Enhanced keyword extraction
    keyword_category = extract_keywords_with_regex(text)
    
    # Check if keyword category matches any existing category
    for category in existing_categories:
        if keyword_category.lower() in category["name"].lower():
            return category["name"]
    
    # Special handling for vehicle-related content
    if any(word in text_lower for word in ['toyota', 'honda', 'bmw', 'mercedes', 'car', 'vehicle', 'auto']):
        return "Vehicles"
    
    return keyword_category if keyword_category != "Other" else "Miscellaneous"

# === 📚 Dynamic Categories ===
def load_categories():
    """Load categories from MongoDB with enhanced default categories"""
    default_categories = [
        {"name": "Electronics", "description": "Devices like phones, laptops, and gadgets"},
        {"name": "Fashion", "description": "Clothing, shoes, and accessories"},
        {"name": "Home Goods", "description": "Furniture, decor, and household items"},
        {"name": "Beauty", "description": "Cosmetics, skincare, and makeup products"},
        {"name": "Sports", "description": "Sporting equipment and gear"},
        {"name": "Books", "description": "Books, novels, and literature"},
        {"name": "Groceries", "description": "Food and grocery items"},
        {"name": "Vehicles", "description": "Cars, bikes, and vehicles"},
        {"name": "Medicine", "description": "Medicines and health remedies"},
        {"name": "Perfume", "description": "Fragrances, colognes, and perfumes"},
        {"name": "Real Estate", "description": "Properties, houses, and apartments"},
        {"name": "Jobs", "description": "Employment opportunities and vacancies"},
        {"name": "Services", "description": "Various services and professional work"},
        {"name": "Urgent", "description": "Urgent sales and time-sensitive offers"}
    ]
    stored_categories = categories_collection.find_one({"_id": "categories"})
    if stored_categories and "categories" in stored_categories:
        return stored_categories["categories"]
    else:
        categories_collection.insert_one({"_id": "categories", "categories": default_categories})
        return default_categories

def save_categories(categories):
    """Save updated categories to MongoDB"""
    categories_collection.update_one(
        {"_id": "categories"},
        {"$set": {"categories": categories}},
        upsert=True
    )

def enrich_product_with_ai(title, desc):
    """Enhanced product enrichment using Hugging Face API with better logic"""
    text = desc if isinstance(desc, str) and len(desc) > 10 else title
    text = clean_text(text)
    
    if not text or len(text) < 10:
        return "Unknown", "No description available"

    # Load current categories
    categories = load_categories()

    # Category classification using API
    try:
        classification_results = ai_classify_text(text, categories)
        
        if classification_results:
            category = propose_new_category(text, classification_results, categories)
        else:
            # Fallback if API fails
            category = extract_keywords_with_regex(text)
            
        # Add new category if it doesn't exist and seems valid
        if (category not in [cat["name"] for cat in categories] and 
            category not in ["Unknown", "Other", "Miscellaneous"] and
            len(category) > 2):
            new_cat_description = f"Products related to {category.lower()}"
            categories.append({"name": category, "description": new_cat_description})
            save_categories(categories)
            print(f"📚 Added new category: {category}")
            
    except Exception as e:
        print(f"⚠️ Classification error: {e}")
        category = extract_keywords_with_regex(text) or "Unknown"

    # Enhanced summarized description using API
    try:
        if len(text) > 50:
            # Use a more focused summary for product descriptions
            summary = ai_summarize_text(text)
            # Clean up the summary to remove redundant information
            summary = re.sub(r'(URGENT SELL|URGENT|SELL|BUY)\s+', '', summary, flags=re.IGNORECASE)
            summary = re.sub(r'\s+', ' ', summary).strip()
        else:
            summary = text
    except Exception as e:
        print(f"⚠️ Summarization error: {e}")
        summary = text[:80] + "..." if len(text) > 80 else text

    return category, summary
# === 🔧 Environment Detection ===
def get_session_filename():
    """Get unique session filename for each environment"""
    if 'RENDER' in os.environ:
        return "session_render.session"
    
    computer_name = platform.node().lower()
    username = os.getenv('USER', '').lower()
    
    local_indicators = ['desktop', 'laptop', 'pc', 'home', 'workstation', 'macbook']
    
    if any(indicator in computer_name for indicator in local_indicators):
        return "session_local.session"
    elif username and username not in ['render', 'root', 'admin']:
        return "session_local.session"
    elif os.name == 'nt':
        return "session_local.session"
    else:
        return "session_main.session"

USER_SESSION_FILE = get_session_filename()
print(f"🔧 Environment detected: Using session file - {USER_SESSION_FILE}")

# === ⚡ MongoDB Setup ===
client = MongoClient(MONGO_URI)
db = client["yetal"]
channels_collection = db["yetalcollection"]
auth_collection = db["authorized_users"]
session_usage_collection = db["session_usage"]
categories_collection = db["categories"]
def error_handler(update, context):
    """Handle errors including Conflict errors"""
    if isinstance(context.error, Conflict):
        print("❌ Conflict error detected - another bot instance might be running")
        return
    print(f'❌ Update "{update}" caused error "{context.error}"')
# === 🔄 AWS S3 File Management Functions (S3 ONLY) ===
def file_exists_in_s3(s3_key):
    """Efficiently check if file exists in S3 using head_object (no download)"""
    try:
        s3.head_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        return True
    except s3.exceptions.NoSuchKey:
        return False
    except Exception as e:
        print(f"❌ Error checking S3 file {s3_key}: {e}")
        return False

def check_s3_files_status():
    """Check existence of all required S3 files efficiently"""
    files_to_check = {
        "Session File": f"sessions/{USER_SESSION_FILE}",
        "Forwarded Messages": f"data/{FORWARDED_FILE}",
        "Scraped Data": f"data/{SCRAPED_DATA_FILE}"
    }
    
    results = {}
    for file_type, s3_key in files_to_check.items():
        exists = file_exists_in_s3(s3_key)
        results[file_type] = exists
        status = "✅" if exists else "❌"
        print(f"{status} {file_type}: {s3_key}")
    
    return results
# JSON data functions - DIRECT S3 access (no local files)
def load_json_from_s3(s3_key):
    """Load JSON data directly from S3 without downloading files"""
    try:
        response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        print(f"✅ Loaded JSON from S3: {s3_key}")
        return data
    except s3.exceptions.NoSuchKey:
        print(f"⚠️ JSON file {s3_key} not found in S3, returning empty dict")
        return {}
    except Exception as e:
        print(f"❌ Error loading JSON from S3: {e}")
        return {}

def save_json_to_s3(data, s3_key):
    """Save JSON data directly to S3 without local files"""
    try:
        # Ensure the folder exists
        folder = s3_key.split('/')[0] + '/'
        try:
            s3.put_object(Bucket=AWS_BUCKET_NAME, Key=folder)
            print(f"✅ Ensured {folder} folder exists in S3")
        except Exception:
            pass  # Folder might already exist
        
        s3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data).encode('utf-8')
        )
        print(f"✅ Saved JSON to S3: {s3_key}")
        return True
    except Exception as e:
        print(f"❌ Error saving JSON to S3: {e}")
        return False

def load_scraped_data_from_s3():
    """Load scraped data directly from S3 JSON file"""
    try:
        data = load_json_from_s3(f"data/{SCRAPED_DATA_FILE}")
        if isinstance(data, list):
            print(f"✅ Loaded {len(data)} records from JSON: {SCRAPED_DATA_FILE}")
            return data
        else:
            print(f"⚠️ JSON file {SCRAPED_DATA_FILE} is not a list, returning empty list")
            return []
    except Exception as e:
        print(f"❌ Error loading scraped data from S3: {e}")
        return []

def save_scraped_data_to_s3(data):
    """Save scraped data to S3 as JSON"""
    try:
        if not data:
            print("⚠️ Data is empty, nothing to save")
            return False
            
        print(f"💾 Attempting to save {len(data)} records to S3 as JSON...")
        
        success = save_json_to_s3(data, f"data/{SCRAPED_DATA_FILE}")
        
        if success:
            print(f"✅ Successfully saved {len(data)} records to S3 as JSON")
            return True
        else:
            print("❌ Failed to save JSON to S3")
            return False
            
    except Exception as e:
        print(f"❌ Error saving to S3: {e}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return False

def ensure_s3_structure():
    """Ensure the required S3 folder structure exists"""
    try:
        # Create sessions folder
        s3.put_object(Bucket=AWS_BUCKET_NAME, Key="sessions/")
        print("✅ Created sessions/ folder in S3")
    except Exception:
        print("✅ sessions/ folder already exists in S3")
    
    try:
        # Create data folder
        s3.put_object(Bucket=AWS_BUCKET_NAME, Key="data/")
        print("✅ Created data/ folder in S3")
    except Exception:
        print("✅ data/ folder already exists in S3")

# === 🧹 Text cleaning and extraction helpers ===
def clean_text(text):
    return ' '.join(text.replace('\xa0', ' ').split())

def extract_info(text, message_id):
    text = clean_text(text)
    
    title_match = re.split(r'\n|💸|☘️☘️PRICE|Price\s*:|💵', text)[0].strip()
    title = title_match[:100] if title_match else "No Title"
    
    phone_matches = re.findall(r'(\+251\d{8,9}|09\d{8})', text)
    phone = phone_matches[0] if phone_matches else ""
    
    price_match = re.search(
        r'(Price|💸|☘️☘️PRICE)[:\s]*([\d,]+)|([\d,]+)\s*(ETB|Birr|birr|💵)', 
        text, 
        re.IGNORECASE
    )
    price = ""
    if price_match:
        price = price_match.group(2) or price_match.group(3) or ""
        price = price.replace(',', '').strip()
    
    location_match = re.search(
        r'(📍|Address|Location|🌺🌺)[:\s]*(.+?)(?=\n|☘️|📞|@|$)', 
        text, 
        re.IGNORECASE
    )
    location = location_match.group(2).strip() if location_match else ""
    
    channel_mention = re.search(r'(@\w+)', text)
    channel_mention = channel_mention.group(1) if channel_mention else ""
    
    return {
        "title": title,
        "description": text,
        "price": price,
        "phone": phone,
        "location": location,
        "channel_mention": channel_mention,
        "product_ref": str(message_id) 
    }

# ======================
# Wrapper for command authorization
# ======================
def authorized(func):
    def wrapper(update, context, *args, **kwargs):
        user_id = update.effective_user.id
        if not auth_collection.find_one({"user_id": user_id}):
            update.message.reply_text(
                "❌ You must enter a valid code first. Use /start to begin."
            )
            return
        return func(update, context, *args, **kwargs)
    return wrapper

# ======================
# Session Usage Tracking
# ======================
def track_session_usage(operation: str, success: bool, error_msg: str = ""):
    """Track session usage for monitoring"""
    try:
        session_usage_collection.insert_one({
            "timestamp": datetime.now(),
            "session_file": USER_SESSION_FILE,
            "environment": "render" if 'RENDER' in os.environ else "local",
            "operation": operation,
            "success": success,
            "error_message": error_msg,
            "computer_name": platform.node()
        })
    except Exception as e:
        print(f"⚠️ Could not track session usage: {e}")

def get_session_usage_stats():
    """Get session usage statistics"""
    try:
        day_ago = datetime.now() - timedelta(hours=24)
        
        total_operations = session_usage_collection.count_documents({
            "timestamp": {"$gte": day_ago}
        })
        
        successful_operations = session_usage_collection.count_documents({
            "timestamp": {"$gte": day_ago},
            "success": True
        })
        
        failed_operations = session_usage_collection.count_documents({
            "timestamp": {"$gte": day_ago},
            "success": False
        })
        
        recent_errors = list(session_usage_collection.find({
            "timestamp": {"$gte": day_ago},
            "success": False
        }).sort("timestamp", -1).limit(5))
        
        env_usage = session_usage_collection.aggregate([
            {"$match": {"timestamp": {"$gte": day_ago}}},
            {"$group": {"_id": "$environment", "count": {"$sum": 1}}}
        ])
        env_usage = {item["_id"]: item["count"] for item in env_usage}
        
        return {
            "total_operations": total_operations,
            "successful_operations": successful_operations,
            "failed_operations": failed_operations,
            "success_rate": (successful_operations / total_operations * 100) if total_operations > 0 else 0,
            "recent_errors": recent_errors,
            "environment_usage": env_usage,
            "current_session": USER_SESSION_FILE,
            "current_environment": "render" if 'RENDER' in os.environ else "local"
        }
    except Exception as e:
        print(f"❌ Error getting session stats: {e}")
        return None

# ======================
# Session Management with S3 (S3 ONLY)
# ======================
def cleanup_telethon_sessions(channel_username=None):
    """Clean up Telethon session files for specific channels (not the main user session)"""
    try:
        if channel_username:
            session_pattern = f"session_{channel_username}.*"
            files = glob.glob(session_pattern)
            for file in files:
                os.remove(file)
                print(f"🧹 Deleted session file: {file}")
        else:
            session_files = glob.glob("session_*.*")
            for file in session_files:
                if file == USER_SESSION_FILE or file.startswith(USER_SESSION_FILE.replace('.session', '')):
                    continue
                os.remove(file)
                print(f"🧹 Deleted session file: {file}")
    except Exception as e:
        print(f"❌ Error cleaning up session files: {e}")

async def get_telethon_client():
    """Get the main Telethon client with READ-ONLY session handling"""
    client = None
    max_retries = 3
    retry_delay = 2
    
    # Download session file from S3 to memory
    session_data = None
    try:
        response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=f"sessions/{USER_SESSION_FILE}")
        session_data = response['Body'].read()
        print(f"✅ Downloaded session file from S3: {USER_SESSION_FILE}")
    except s3.exceptions.NoSuchKey:
        print(f"❌ Session file not found in S3: {USER_SESSION_FILE}")
        return None
    except Exception as e:
        print(f"❌ Error downloading session from S3: {e}")
        return None
    
    for attempt in range(max_retries):
        try:
            print(f"🔧 Attempt {attempt + 1}/{max_retries} to connect Telethon client...")
            
            # 🚀 FIX: Create a temporary writable session file
            temp_session_file = f"temp_{USER_SESSION_FILE}"
            with open(temp_session_file, 'wb') as f:
                f.write(session_data)
            
            # 🚀 FIX: Set proper permissions for the temp file
            try:
                os.chmod(temp_session_file, 0o644)
            except:
                pass  # Ignore permission errors on some systems
            
            # Use the temporary session file
            session = SQLiteSession(temp_session_file)
            client = TelegramClient(session, API_ID, API_HASH)
            
            await asyncio.wait_for(client.connect(), timeout=15)
            
            if not await client.is_user_authorized():
                error_msg = "Session not authorized"
                print(f"❌ {error_msg}")
                track_session_usage("connection", False, error_msg)
                await client.disconnect()
                # Clean up temporary file
                if os.path.exists(temp_session_file):
                    os.remove(temp_session_file)
                return None
            
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            print(f"✅ Telethon connected successfully as: {me.first_name} (@{me.username})")
            track_session_usage("connection", True)
            
            # 🚀 FIX: Store the temp file name for cleanup
            client.temp_session_file = temp_session_file
            return client
            
        except Exception as e:
            error_msg = f"Connection error (attempt {attempt + 1}): {str(e)}"
            print(f"❌ {error_msg}")
            track_session_usage("connection", False, error_msg)
            
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            
            # Clean up temporary file on error
            if 'temp_session_file' in locals() and os.path.exists(temp_session_file):
                try:
                    os.remove(temp_session_file)
                except:
                    pass
            
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                return None
    
    return None
# ======================
# Forward last 7d posts with PURE S3 integration
# ======================
async def forward_last_7d_async(channel_username: str):
    """Async function to forward messages using the main Telethon client"""
    telethon_client = None
    
    try:
        # Use direct S3 access for forwarded data
        print("🔍 Loading forwarded messages data directly from S3...")
        
        telethon_client = await get_telethon_client()
        if not telethon_client:
            error_msg = "Failed to initialize Telethon client after retries"
            track_session_usage("forwarding", False, error_msg)
            return False, "❌ Could not establish connection. Please try again or check /checksessionusage."
        
        print(f"🔍 Checking if channel {channel_username} exists...")
        
        try:
            entity = await asyncio.wait_for(
                telethon_client.get_entity(channel_username), 
                timeout=15
            )
            print(f"✅ Channel found: {entity.title}")
        except (ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            error_msg = f"Invalid channel: {str(e)}"
            track_session_usage("forwarding", False, error_msg)
            return False, f"❌ Channel {channel_username} is invalid or doesn't exist."
        except asyncio.TimeoutError:
            error_msg = "Timeout accessing channel"
            track_session_usage("forwarding", False, error_msg)
            return False, f"❌ Timeout accessing channel {channel_username}"
        except Exception as e:
            error_msg = f"Error accessing channel: {str(e)}"
            track_session_usage("forwarding", False, error_msg)
            return False, f"❌ Error accessing channel: {str(e)}"

        # Verify target channel
        try:
            target_entity = await telethon_client.get_entity(FORWARD_CHANNEL)
            print(f"✅ Target channel: {target_entity.title}")
        except Exception as e:
            error_msg = f"Cannot access target channel: {str(e)}"
            track_session_usage("forwarding", False, error_msg)
            return False, f"❌ Cannot access target channel: {str(e)}"

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        print(f"⏰ Forwarding messages since: {cutoff}")

        # Load previously forwarded messages with timestamps - DIRECT FROM S3
        all_forwarded_data = load_json_from_s3(f"data/{FORWARDED_FILE}")
        if not isinstance(all_forwarded_data, dict):
            all_forwarded_data = {}
        
        # Get or initialize for this channel
        channel_forwarded = all_forwarded_data.get(channel_username, {})
        
        forwarded_ids = {
            int(msg_id): datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") 
            for msg_id, ts in channel_forwarded.items()
        } if channel_forwarded else {}

        # Remove forwarded IDs older than 7 days
        week_cutoff = now - timedelta(days=7)
        forwarded_ids = {msg_id: ts for msg_id, ts in forwarded_ids.items() 
                         if ts >= week_cutoff.replace(tzinfo=None)}

        messages_to_forward = []
        message_count = 0
        
        print(f"📨 Fetching messages from {channel_username}...")
        
        try:
            async for message in telethon_client.iter_messages(entity, limit=None):
                message_count += 1
                if message_count % 10 == 0:
                    print(f"📊 Processed {message_count} messages...")
                    
                if message.date < cutoff:
                    print(f"⏹️ Reached cutoff time at message {message_count}")
                    break
                    
                # Check if message is already forwarded and has content
                if message.id not in forwarded_ids and (message.text or message.media):
                    messages_to_forward.append(message)
                    print(f"✅ Added message {message.id} from {message.date}")

        except Exception as e:
            print(f"⚠️ Error fetching messages: {e}")

        print(f"📋 Found {len(messages_to_forward)} new messages to forward")

        if not messages_to_forward:
            track_session_usage("forwarding", True, "No new messages to forward")
            return False, f"📭 No new posts found in the last 7d from {channel_username}."

        # Reverse to forward in chronological order
        messages_to_forward.reverse()
        total_forwarded = 0
        
        print(f"➡️ Forwarding {len(messages_to_forward)} messages from {channel_username}...")
        
        # Forward in batches of 10 to avoid rate limits
        for i in range(0, len(messages_to_forward), 10):
            batch = messages_to_forward[i:i+10]
            try:
                await asyncio.wait_for(
                    telethon_client.forward_messages(
                        entity=FORWARD_CHANNEL,
                        messages=[msg.id for msg in batch],
                        from_peer=channel_username
                    ),
                    timeout=30
                )
                
                # Update forwarded IDs
                for msg in batch:
                    forwarded_ids[msg.id] = msg.date.replace(tzinfo=None)
                    total_forwarded += 1
                
                print(f"✅ Forwarded batch {i//10 + 1}/{(len(messages_to_forward)-1)//10 + 1} ({len(batch)} messages)")
                await asyncio.sleep(1)
                
            except ChatForwardsRestrictedError:
                print(f"🚫 Forwarding restricted for channel {channel_username}, skipping...")
                break
            except FloodWaitError as e:
                print(f"⏳ Flood wait error ({e.seconds}s). Waiting...")
                await asyncio.sleep(e.seconds)
                continue
            except asyncio.TimeoutError:
                print(f"⚠️ Forwarding timed out for {channel_username}, skipping batch...")
                continue
            except RPCError as e:
                print(f"⚠️ RPC Error for {channel_username}: {e}")
                continue
            except Exception as e:
                print(f"⚠️ Unexpected error forwarding from {channel_username}: {e}")
                continue

        # Update channel data and save entire structure DIRECTLY TO S3
        all_forwarded_data[channel_username] = {
            str(k): v.strftime("%Y-%m-%d %H:%M:%S") for k, v in forwarded_ids.items()
        }
        save_json_to_s3(all_forwarded_data, f"data/{FORWARDED_FILE}")

        if total_forwarded > 0:
            track_session_usage("forwarding", True, f"Forwarded {total_forwarded} messages")
            return True, f"✅ Successfully forwarded {total_forwarded} new posts from {channel_username}."
        else:
            track_session_usage("forwarding", False, "No messages forwarded")
            return False, f"📭 No new posts to forward from {channel_username}."

    except Exception as e:
        error_msg = f"Critical error: {str(e)}"
        print(f"❌ {error_msg}")
        track_session_usage("forwarding", False, error_msg)
        return False, f"❌ Critical error: {str(e)}"
    finally:
        # Upload session file to S3 after operations
        if telethon_client:
            try:
                await telethon_client.disconnect()
                print("📤 Uploading updated session file to S3...")
                # Read the updated session file and upload to S3
                if os.path.exists(USER_SESSION_FILE):
                    with open(USER_SESSION_FILE, 'rb') as f:
                        s3.put_object(
                            Bucket=AWS_BUCKET_NAME,
                            Key=f"sessions/{USER_SESSION_FILE}",
                            Body=f.read()
                        )
                    print(f"✅ Session file uploaded to S3: {USER_SESSION_FILE}")
                    # Clean up temporary file
                    os.remove(USER_SESSION_FILE)
            except Exception as e:
                print(f"⚠️ Error uploading session file: {e}")

def forward_last_7d_sync(channel_username: str):
    """Synchronous wrapper for the async forwarding function"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(forward_last_7d_async(channel_username))
        loop.close()
        return result
    except Exception as e:
        track_session_usage("forwarding", False, f"Sync error: {str(e)}")
        return False, f"❌ Error: {str(e)}"
# ======================
# Session management commands with S3
# ======================
@authorized
def setup_session(update, context):
    """Command to set up the user session (run this once manually)"""
    def run_session_setup():
        try:
            async def setup_async():
                client = None
                try:
                    client = TelegramClient(USER_SESSION_FILE, API_ID, API_HASH)
                    await client.start()
                    
                    me = await client.get_me()
                    result = f"✅ Session setup successful!\nLogged in as: {me.first_name} (@{me.username})\n\nSession file: {USER_SESSION_FILE}"
                    track_session_usage("setup", True)
                    
                    # Upload session to S3 after setup
                    print("📤 Uploading new session to S3...")
                    with open(USER_SESSION_FILE, 'rb') as f:
                        s3.put_object(
                            Bucket=AWS_BUCKET_NAME,
                            Key=f"sessions/{USER_SESSION_FILE}",
                            Body=f.read()
                        )
                    print(f"✅ Session file uploaded to S3: {USER_SESSION_FILE}")
                    
                    return result
                except Exception as e:
                    error_msg = f"Session setup failed: {e}"
                    track_session_usage("setup", False, error_msg)
                    return f"❌ {error_msg}"
                finally:
                    if client:
                        await client.disconnect()
                    # Clean up local session file
                    if os.path.exists(USER_SESSION_FILE):
                        os.remove(USER_SESSION_FILE)
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(setup_async())
            loop.close()
            
            context.bot.send_message(update.effective_chat.id, text=result)
            
        except Exception as e:
            context.bot.send_message(update.effective_chat.id, text=f"❌ Session setup error: {e}")
    
    update.message.reply_text("🔐 Starting session setup... This may require phone number verification.")
    threading.Thread(target=run_session_setup, daemon=True).start()

@authorized
def debug_json_comprehensive(update, context):
    """Comprehensive debug command to check S3 JSON files"""
    try:
        s3_key = f"data/{SCRAPED_DATA_FILE}"
        
        msg = f"🔍 <b>Comprehensive JSON Debug (S3 ONLY)</b>\n\n"
        
        # Check S3 file
        s3_exists = file_exists_in_s3(s3_key)
        msg += f"☁️ <b>S3 Status:</b> {'✅ Exists' if s3_exists else '❌ Missing'}\n"
        
        if s3_exists:
            try:
                response = s3.head_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
                s3_size = response['ContentLength']
                s3_modified = response['LastModified']
                msg += f"📏 <b>S3 Size:</b> {s3_size} bytes\n"
                msg += f"🕒 <b>S3 Modified:</b> {s3_modified}\n"
                
                # Try to load S3 data
                s3_data = load_scraped_data_from_s3()
                if s3_data:
                    msg += f"📊 <b>S3 Data:</b> {len(s3_data)} records\n"
                    
                    # Convert to DataFrame for analysis
                    import pandas as pd
                    df = pd.DataFrame(s3_data)
                    
                    msg += f"\n📈 <b>Data Summary:</b>\n"
                    msg += f"• Total Records: {len(s3_data)}\n"
                    if 'date' in df.columns:
                        msg += f"• Date Range: {df['date'].min() if 'date' in df.columns else 'N/A'} to {df['date'].max() if 'date' in df.columns else 'N/A'}\n"
                    if 'channel' in df.columns:
                        msg += f"• Channels: {df['channel'].nunique() if 'channel' in df.columns else 'N/A'}\n"
                    
                    if 'channel' in df.columns:
                        msg += f"\n🔍 <b>Top Channels:</b>\n"
                        channel_counts = df['channel'].value_counts().head(3)
                        for channel, count in channel_counts.items():
                            msg += f"• {channel}: {count} records\n"
                else:
                    msg += "⚠️ S3 file exists but contains no data\n"
            except Exception as e:
                msg += f"❌ <b>S3 Error:</b> {e}\n"
        
        update.message.reply_text(msg, parse_mode="HTML")
        
    except Exception as e:
        update.message.reply_text(f"❌ Comprehensive debug error: {e}")

@authorized
def check_session(update, context):
    """Check if the user session is valid"""
    def run_check():
        try:
            async def check_async():
                client = None
                try:
                    client = await get_telethon_client()
                    if not client:
                        return "❌ Session connection failed. Check /checksessionusage for details."
                    
                    me = await client.get_me()
                    result = f"✅ Session is valid!\nLogged in as: {me.first_name} (@{me.username})\n\nSession file: {USER_SESSION_FILE}"
                    track_session_usage("check", True)
                    return result
                except Exception as e:
                    error_msg = f"Session check failed: {e}"
                    track_session_usage("check", False, error_msg)
                    return f"❌ {error_msg}"
                finally:
                    if client:
                        await client.disconnect()
                    # Clean up temporary session file
                    if os.path.exists(USER_SESSION_FILE):
                        os.remove(USER_SESSION_FILE)
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(check_async())
            loop.close()
            
            context.bot.send_message(update.effective_chat.id, text=result)
            
        except Exception as e:
            context.bot.send_message(update.effective_chat.id, text=f"❌ Session check error: {e}")
    
    threading.Thread(target=run_check, daemon=True).start()

@authorized
def check_session_usage(update, context):
    """Check session usage statistics and health"""
    try:
        stats = get_session_usage_stats()
        if not stats:
            update.message.reply_text("❌ Could not retrieve session usage statistics.")
            return
        
        msg = f"📊 <b>Session Usage Statistics (Last 24h)</b>\n\n"
        msg += f"🔧 <b>Current Session:</b> {stats['current_session']}\n"
        msg += f"🌍 <b>Environment:</b> {stats['current_environment']}\n"
        msg += f"💻 <b>Computer:</b> {platform.node()}\n"
        msg += f"☁️ <b>S3 Bucket:</b> {AWS_BUCKET_NAME}\n\n"
        
        msg += f"📈 <b>Operations Summary:</b>\n"
        msg += f"• Total Operations: {stats['total_operations']}\n"
        msg += f"• Successful: {stats['successful_operations']}\n"
        msg += f"• Failed: {stats['failed_operations']}\n"
        msg += f"• Success Rate: {stats['success_rate']:.1f}%\n\n"
        
        msg += f"🌍 <b>Environment Usage:</b>\n"
        for env, count in stats['environment_usage'].items():
            msg += f"• {env}: {count} operations\n"
        
        if stats['recent_errors']:
            msg += f"\n⚠️ <b>Recent Errors (last 5):</b>\n"
            for error in stats['recent_errors']:
                timestamp = error['timestamp'].strftime("%H:%M:%S")
                operation = error['operation']
                error_msg = error['error_message'][:50] + "..." if len(error['error_message']) > 50 else error['error_message']
                msg += f"• {timestamp} - {operation}: {error_msg}\n"
        
        # Add health status
        if stats['success_rate'] >= 90:
            health = "🟢 Excellent"
        elif stats['success_rate'] >= 75:
            health = "🟡 Good"
        elif stats['success_rate'] >= 50:
            health = "🟠 Fair"
        else:
            health = "🔴 Poor"
            
        msg += f"\n❤️ <b>Health Status:</b> {health}"
        
        update.message.reply_text(msg, parse_mode="HTML")
        
    except Exception as e:
        update.message.reply_text(f"❌ Error checking session usage: {e}")
# ======================
# 7-day scraping function with PURE S3 integration - FIXED CUTOFF
# ======================
async def scrape_channel_7days_async(channel_username: str):
    """Scrape last 7 days of data and link to forwarded messages in target channel - STRICT 7-DAY CUTOFF"""
    telethon_client = None
    
    try:
        print("Loading json data directly from S3...")
        
        telethon_client = await get_telethon_client()
        if not telethon_client:
            track_session_usage("scraping", False, "Failed to initialize client")
            return False, "Could not establish connection for scraping."
        
        print(f"Starting 7-day scrape with AI enrichment for channel: {channel_username}")
        
        try:
            # Get the SOURCE channel entity (where we scrape FROM)
            source_entity = await telethon_client.get_entity(channel_username)
            print(f"Source channel found: {source_entity.title}")
            
            # Get the TARGET channel entity (where messages are forwarded TO)
            target_entity = await telethon_client.get_entity(FORWARD_CHANNEL)
            print(f"Target channel found: {target_entity.title}")
            
        except (ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            track_session_usage("scraping", False, f"Invalid channel: {str(e)}")
            return False, f"Channel {channel_username} is invalid or doesn't exist."

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        print(f"STRICT 7-DAY CUTOFF: Scraping ONLY messages since {cutoff}")

        # Load forwarded data to get recent source IDs - ONLY FROM LAST 7 DAYS
        all_forwarded_data = load_json_from_s3(f"data/{FORWARDED_FILE}")
        if not isinstance(all_forwarded_data, dict):
            all_forwarded_data = {}
        
        channel_forwarded = all_forwarded_data.get(channel_username, {})
        recent_source_ids = set()
        
        # CRITICAL FIX: Only include source IDs from the last 7 days
        for source_str, ts_str in channel_forwarded.items():
            try:
                source_id = int(source_str)
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                # ONLY include IDs from the last 7 days
                if ts >= cutoff:
                    recent_source_ids.add(source_id)
            except Exception as e:
                print(f"Error parsing forwarded data for {source_str}: {e}")
                continue
        
        print(f"Found {len(recent_source_ids)} forwarded source IDs within 7 days")
        
        if not recent_source_ids:
            print(f"No recent forwarded messages found for {channel_username} in the last 7 days.")
            return False, f"No recent forwards found for {channel_username} in the last 7 days."

        # DIRECT SCRAPING from TARGET channel with proper cutoff (like your working code)
        scraped_data = []
        seen_posts = set()
        
        print(f"📡 Scraping target channel for {channel_username} messages...")
        
        # Use the same logic as your working code - iterate messages with cutoff break
        async for message in telethon_client.iter_messages(target_entity, limit=None):
            # Skip messages without text
            if not message.text:
                continue

            # STRICT CUTOFF: Break when we reach messages older than 7 days
            if message.date < cutoff:
                print(f"Reached 7-day cutoff in target channel at message {message.id}")
                break

            if message.id in seen_posts:
                continue
            seen_posts.add(message.id)

            # Extract info from the forwarded message
            info = extract_info(message.text, message.id)
            
            # Try to find matching source ID in the recent forwarded IDs
            matched_source_id = None
            for source_id in recent_source_ids:
                # Simple check - if we can't find a clean way to match, use the first available
                # This maintains your original product_ref logic
                matched_source_id = source_id
                break  # Use the first matching source ID

            if not matched_source_id:
                continue

            # AI ENHANCEMENT
            print(f"AI enriching message {message.id}: {info['title'][:50]}...")
            predicted_category, generated_description = enrich_product_with_ai(info["title"], info["description"])            
            
            # Build permalink from TARGET channel (keep your original logic)
            if getattr(target_entity, "username", None):
                post_link = f"https://t.me/{target_entity.username}/{message.id}"
            else:
                internal_id = str(target_entity.id)
                if internal_id.startswith("-100"):
                    internal_id = internal_id[4:]
                post_link = f"https://t.me/c/{internal_id}/{message.id}"

            # Keep your original product_ref format
            product_ref = f"{channel_username}_{matched_source_id}"

            post_data = {
                "title": info["title"],
                "description": info["description"],
                "price": info["price"],
                "phone": info["phone"],
                "location": info["location"],
                "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                "channel": channel_username,
                "post_link": post_link,   
                "product_ref": product_ref,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "predicted_category": predicted_category,
                "generated_description": generated_description,
                "ai_enhanced": True,
                "has_media": bool(message.media),
                "has_text": bool(message.text),
                "source_message_id": matched_source_id,
                "target_message_id": message.id
            }
            scraped_data.append(post_data)

        # Final filter - DOUBLE CHECK everything is within 7 days
        final_scraped_data = []
        for post in scraped_data:
            post_date = datetime.strptime(post["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if post_date >= cutoff:
                final_scraped_data.append(post)
            else:
                print(f"Filtered out post {post['target_message_id']} from {post_date} (older than 7 days)")

        print(f"Found {len(final_scraped_data)} messages with AI enhancement from {channel_username} (within 7 days)")
        
        # Load existing data DIRECTLY FROM S3 JSON
        existing_data = load_scraped_data_from_s3()
        
        print(f"Loaded existing data with {len(existing_data)} records from S3 JSON")

        if final_scraped_data:
            # Combine and deduplicate
            combined_data = existing_data.copy()
            existing_refs = {item['product_ref'] for item in existing_data}
            new_items_added = 0
            
            for new_item in final_scraped_data:
                if new_item['product_ref'] not in existing_refs:
                    combined_data.append(new_item)
                    new_items_added += 1
            
            # Save to S3
            success = save_scraped_data_to_s3(combined_data)
            if success:
                print(f"Saved {len(combined_data)} total records to S3 with AI enhancement")
                track_session_usage("scraping", True, f"Scraped {len(final_scraped_data)} messages with AI")
                result_msg = f"Scraped {len(final_scraped_data)} messages from {channel_username}. Added {new_items_added} new records."
                return True, result_msg
            else:
                track_session_usage("scraping", False, "S3 save failed")
                return False, f"Failed to save data for {channel_username} to S3."
        else:
            track_session_usage("scraping", True, "No new messages found")
            return False, f"No messages found for {channel_username} in the last 7 days."
            
    except Exception as e:
        error_msg = f"Scraping error: {str(e)}"
        print(f"{error_msg}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        track_session_usage("scraping", False, error_msg)
        return False, f"Scraping error: {str(e)}"
    finally:
        if telethon_client:
            try:
                await telethon_client.disconnect()
                print("Uploading updated session file to S3...")
                if os.path.exists(USER_SESSION_FILE):
                    with open(USER_SESSION_FILE, 'rb') as f:
                        s3.put_object(
                            Bucket=AWS_BUCKET_NAME,
                            Key=f"sessions/{USER_SESSION_FILE}",
                            Body=f.read()
                        )
                    print(f"Session file uploaded to S3: {USER_SESSION_FILE}")
                    os.remove(USER_SESSION_FILE)
            except Exception as e:
                print(f"Error during cleanup: {e}")

def scrape_channel_7days_sync(channel_username: str):
    """Synchronous wrapper for 7-day scraping"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(scrape_channel_7days_async(channel_username))
        loop.close()
        return result
    except Exception as e:
        track_session_usage("scraping", False, f"Sync error: {str(e)}")
        return False, f"❌ Scraping error: {str(e)}"
# ======================
# Bot commands (remain the same but now use pure S3 functions)
# ======================
@authorized
def add_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /addchannel @ChannelUsername")
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text("❌ Please provide a valid channel username starting with @")
        return

    if channels_collection.find_one({"username": username}):
        update.message.reply_text("⚠️ This channel is already saved in the database.")
        return

    try:
        chat = context.bot.get_chat(username)
        channels_collection.insert_one({"username": username, "title": chat.title})
        update.message.reply_text(
            f"✅ <b>Channel saved successfully!</b>\n\n"
            f"📌 <b>Name:</b> {chat.title}\n"
            f"🔗 <b>Username:</b> {username}",
            parse_mode="HTML",
        )

        # Run operations in CORRECT ORDER
        def run_operations():
            try:
                # FIRST: Forward messages to target channel
                update.message.reply_text(f"⏳ Forwarding last 7d posts from {username}...")
                success, result_msg = forward_last_7d_sync(username)
                context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
                
                # Add delay to ensure forwarding completes
                import time
                time.sleep(3)
                
                # THEN: Scrape from target channel (now messages exist there)
                update.message.reply_text(f"⏳ Starting 7-day data scraping from {username}...")
                success, result_msg = scrape_channel_7days_sync(username)
                context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
                
            except Exception as e:
                error_msg = f"❌ Error during operations: {str(e)}"
                context.bot.send_message(update.effective_chat.id, text=error_msg, parse_mode="HTML")

        threading.Thread(target=run_operations, daemon=True).start()

    except BadRequest as e:
        update.message.reply_text(f"❌ Could not add channel: {str(e)}")
    except Exception as e:
        update.message.reply_text(f"❌ Unexpected error: {str(e)}")
@authorized
def debug_s3_json(update, context):
    """Enhanced debug command to check S3 JSON file status"""
    try:
        s3_key = f"data/{SCRAPED_DATA_FILE}"
        
        # Check if file exists in S3
        exists = file_exists_in_s3(s3_key)
        msg = f"🔍 <b>S3 JSON Debug Information</b>\n\n"
        msg += f"📁 <b>S3 Path:</b> {s3_key}\n"
        msg += f"✅ <b>File Exists:</b> {'Yes' if exists else 'No'}\n\n"
        
        if exists:
            # Get file info
            try:
                response = s3.head_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
                size_mb = response['ContentLength'] / (1024 * 1024)
                last_modified = response['LastModified']
                msg += f"📏 <b>File Size:</b> {size_mb:.2f} MB\n"
                msg += f"🕒 <b>Last Modified:</b> {last_modified}\n\n"
            except Exception as e:
                msg += f"⚠️ <b>File Info Error:</b> {e}\n\n"
            
            # Try to load and show data
            try:
                data = load_scraped_data_from_s3()
                if data:
                    msg += f"📊 <b>Data Summary:</b>\n"
                    msg += f"• Total Records: {len(data)}\n"
                    
                    # Convert to DataFrame for analysis
                    import pandas as pd
                    df = pd.DataFrame(data)
                    
                    if 'date' in df.columns:
                        msg += f"• Date Range: {df['date'].min()} to {df['date'].max()}\n"
                    if 'channel' in df.columns:
                        msg += f"• Channels: {df['channel'].nunique()}\n\n"
                    
                    if 'channel' in df.columns:
                        msg += f"🔍 <b>Channel Distribution:</b>\n"
                        channel_counts = df['channel'].value_counts()
                        for channel, count in channel_counts.head(5).items():
                            msg += f"• {channel}: {count} records\n"
                else:
                    msg += "⚠️ File exists but contains no data\n"
            except Exception as e:
                msg += f"❌ <b>Data Load Error:</b> {e}\n"
        else:
            msg += "💡 <b>Solution:</b> Run scraping to create the file\n"
        
        update.message.reply_text(msg, parse_mode="HTML")
        
    except Exception as e:
        update.message.reply_text(f"❌ Debug error: {e}")
@authorized
def check_scraped_data(update, context):
    """Check the current scraped data statistics from S3 with AI insights"""
    try:
        data = load_scraped_data_from_s3()
        if data:
            df = pd.DataFrame(data)
            channel_counts = df['channel'].value_counts()
            
            msg = f"📊 <b>Scraped Data Summary (AI Enhanced):</b>\n"
            msg += f"Total records: {len(df)}\n"
            
            # AI Enhancement stats
            if 'predicted_category' in df.columns:
                ai_enhanced_count = df['ai_enhanced'].sum() if 'ai_enhanced' in df.columns else len(df)
                unique_categories = df['predicted_category'].nunique()
                msg += f"AI Enhanced: {ai_enhanced_count} records\n"
                msg += f"Unique Categories: {unique_categories}\n\n"
            
            msg += "<b>Records per channel:</b>\n"
            for channel, count in channel_counts.items():
                msg += f"• {channel}: {count} records\n"
            
            # Show top categories if available
            if 'predicted_category' in df.columns:
                msg += f"\n<b>Top Categories:</b>\n"
                category_counts = df['predicted_category'].value_counts().head(5)
                for category, count in category_counts.items():
                    msg += f"• {category}: {count} products\n"
                
            update.message.reply_text(msg, parse_mode="HTML")
        else:
            update.message.reply_text("📭 No scraped data found in S3 yet.")
    except Exception as e:
        update.message.reply_text(f"❌ Error checking scraped data: {e}")
@authorized
def list_channels(update, context):
    channels = list(channels_collection.find({}))
    if not channels:
        update.message.reply_text("📭 No channels saved yet.")
        return

    msg_lines = ["📃 <b>Saved Channels:</b>\n"]
    for ch in channels:
        username = ch.get("username")
        title = ch.get("title", "Unknown")
        msg_lines.append(f"{username} — <b>{title}</b>")

    msg = "\n".join(msg_lines)
    for chunk in [msg[i : i + 4000] for i in range(0, len(msg), 4000)]:
        update.message.reply_text(chunk, parse_mode="HTML")

@authorized
def check_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /checkchannel @ChannelUsername")
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text("❌ Please provide a valid channel username starting with @")
        return

    doc = channels_collection.find_one({"username": username})
    if doc:
        update.message.reply_text(
            f"🔍 <b>Channel found in database!</b>\n\n"
            f"📌 <b>Name:</b> {doc.get('title', 'Unknown')}\n"
            f"🔗 <b>Username:</b> {username}",
            parse_mode="HTML",
        )
    else:
        update.message.reply_text(f"❌ Channel {username} is not in the database.", parse_mode="HTML")

@authorized
def delete_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /deletechannel @ChannelUsername")
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text("❌ Please provide a valid channel username starting with @")
        return

    result = channels_collection.delete_one({"username": username})
    if result.deleted_count > 0:
        update.message.reply_text(f"✅ Channel {username} has been deleted from the database.")
    else:
        update.message.reply_text(f"⚠️ Channel {username} was not found in the database.")
@authorized
def delete_s3_files(update, context):
    """Delete the scraped_data.json and forwarded_messages.json files from S3"""
    try:
        deleted_files = []
        errors = []
        
        # List of files to delete
        files_to_delete = [
            f"data/{SCRAPED_DATA_FILE}",  # scraped_data.json
            f"data/{FORWARDED_FILE}"      # forwarded_messages.json
        ]
        
        for s3_key in files_to_delete:
            try:
                # Check if exists before deleting
                if file_exists_in_s3(s3_key):
                    s3.delete_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
                    deleted_files.append(s3_key)
                    print(f"Deleted {s3_key} from S3")
                else:
                    errors.append(f"{s3_key} not found in S3")
            except Exception as e:
                errors.append(f"Error deleting {s3_key}: {str(e)}")
        
        # Prepare response
        msg = f"<b>S3 File Deletion Report</b>\n\n"
        if deleted_files:
            msg += f"<b>Deleted:</b>\n"
            for f in deleted_files:
                msg += f"• {f}\n"
        if errors:
            msg += f"\n<b>Issues:</b>\n"
            for e in errors:
                msg += f"• {e}\n"
        
        if not deleted_files and not errors:
            msg += "No files were targeted or found."
        
        update.message.reply_text(msg, parse_mode="HTML")
        
        track_session_usage("delete_s3_files", True if deleted_files else False, f"Deleted: {len(deleted_files)}, Errors: {len(errors)}")
        
    except Exception as e:
        error_msg = f"Unexpected error during deletion: {str(e)}"
        update.message.reply_text(f"Failed: {error_msg}")
        print(error_msg)
        track_session_usage("delete_s3_files", False, error_msg)
@authorized
def check_s3_status(update, context):
    """Check S3 bucket and file status efficiently"""
    try:
        # Test S3 connection
        try:
            s3.head_bucket(Bucket=AWS_BUCKET_NAME)
            bucket_status = "✅ Connected"
        except Exception as e:
            bucket_status = f"❌ Error: {e}"
        
        # Check files efficiently
        files_status = check_s3_files_status()
        
        msg = f"☁️ <b>S3 Status Report (Efficient Check)</b>\n\n"
        msg += f"<b>Bucket Connection:</b> {bucket_status}\n"
        msg += f"<b>Bucket Name:</b> {AWS_BUCKET_NAME}\n\n"
        
        msg += f"<b>File Status (using head_object):</b>\n"
        for file_type, exists in files_status.items():
            status = "✅ Exists" if exists else "❌ Missing"
            msg += f"• {file_type}: {status}\n"
        
        # Add folder structure info
        msg += f"\n<b>Expected S3 Structure:</b>\n"
        msg += f"• {AWS_BUCKET_NAME}/\n"
        msg += f"  ├── sessions/\n"
        msg += f"  │   └── {USER_SESSION_FILE}\n"
        msg += f"  └── data/\n"
        msg += f"      ├── {FORWARDED_FILE}\n"
        msg += f"      └── {SCRAPED_DATA_FILE}\n"
        
        update.message.reply_text(msg, parse_mode="HTML")
        
    except Exception as e:
        update.message.reply_text(f"❌ Error checking S3 status: {e}")

@authorized
def unknown_command(update, context):
    update.message.reply_text(
        "❌ Unknown command.\n\n"
        "👉 Available commands:\n"
        "/addchannel @ChannelUsername\n"
        "/listchannels\n"
        "/checkchannel @ChannelUsername\n"
        "/deletechannel @ChannelUsername\n"
        "/setup v3 - Set up Telegram session\n"
        "/check_session - Check session status\n"
        "/checksessionusage - Session usage stats\n"
        "/test - Test connection\n"
        "/check_data - Check scraped data\n"
        "/check_s3 - Check S3 status\n"
        "/cleanup - Cleanup sessions\n"
        "/clearhistory - Clear forwarded history"
        "/diagnose - Diagnose session health\n"
        "/debug_json - Debug a json file in S3\n"
        "/test_s3_write - Test S3 write access\n"
        "/debug_json_comprehensive - Comprehensive json debug\n"
        "/getscrapedjson - get json file\n"
        "/deletes3files - Delete S3 files\n"
    )

@authorized
def cleanup_sessions(update, context):
    """Clean up all temporary Telethon session files (except main user session)"""
    try:
        cleanup_telethon_sessions()
        update.message.reply_text("✅ All temporary session files have been cleaned up.")
    except Exception as e:
        update.message.reply_text(f"❌ Error cleaning up sessions: {e}")

@authorized
def clear_forwarded_history(update, context):
    """Clear the forwarded messages history from S3"""
    try:
        # Delete the forwarded file from S3
        try:
            s3.delete_object(Bucket=AWS_BUCKET_NAME, Key=f"data/{FORWARDED_FILE}")
            update.message.reply_text("✅ Forwarded messages history cleared from S3.")
        except Exception as e:
            update.message.reply_text(f"❌ Error clearing history from S3: {e}")
    except Exception as e:
        update.message.reply_text(f"❌ Error clearing history: {e}")

@authorized
def test_connection(update, context):
    """Test if Telethon client is working with S3"""
    def run_test():
        try:
            async def test_async():
                client = None
                try:
                    # Test S3 connection first
                    try:
                        s3.head_bucket(Bucket=AWS_BUCKET_NAME)
                        s3_status = "✅ S3 connection successful"
                    except Exception as e:
                        s3_status = f"❌ S3 connection failed: {e}"
                    
                    client = await get_telethon_client()
                    if not client:
                        return f"{s3_status}\n❌ Could not establish Telethon connection."
                    
                    me = await client.get_me()
                    result = f"{s3_status}\n✅ Telethon connected as: {me.first_name} (@{me.username})\n\n"
                    
                    try:
                        target = await client.get_entity(FORWARD_CHANNEL)
                        result += f"✅ Target channel accessible: {target.title}"
                    except Exception as e:
                        result += f"❌ Cannot access target channel {FORWARD_CHANNEL}: {e}"
                    
                    track_session_usage("test", True)
                    return result
                except Exception as e:
                    error_msg = f"Telethon connection error: {e}"
                    track_session_usage("test", False, error_msg)
                    return f"❌ {error_msg}"
                finally:
                    if client:
                        await client.disconnect()
                    # Clean up temporary session file
                    if os.path.exists(USER_SESSION_FILE):
                        os.remove(USER_SESSION_FILE)
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_async())
            loop.close()
            
            context.bot.send_message(update.effective_chat.id, text=result)
            
        except Exception as e:
            context.bot.send_message(update.effective_chat.id, text=f"❌ Test failed: {e}")
    
    threading.Thread(target=run_test, daemon=True).start()
@authorized
def get_scraped_json(update, context):
    """Download and send the scraped JSON file directly from S3 to Telegram"""
    try:
        s3_key = f"data/{SCRAPED_DATA_FILE}"
        
        # Check if file exists in S3
        if not file_exists_in_s3(s3_key):
            update.message.reply_text(f"File {SCRAPED_DATA_FILE} not found in S3.")
            return
        
        # Download the file content directly from S3
        response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        json_content = response['Body'].read()
        
        # Create a BytesIO buffer to send as document
        buffer = io.BytesIO(json_content)
        buffer.name = SCRAPED_DATA_FILE  # Set filename for Telegram
        
        # Send the file
        update.message.reply_document(
            document=buffer,
            filename=SCRAPED_DATA_FILE,
            caption=f"Here is the current scraped data from S3.\nSize: {len(json_content)} bytes"
        )
        
        print(f"Sent {SCRAPED_DATA_FILE} to user {update.effective_user.id}")
        track_session_usage("get_json", True, f"Sent {SCRAPED_DATA_FILE}")
        
    except Exception as e:
        error_msg = f"Error sending JSON file: {str(e)}"
        update.message.reply_text(f"Failed to retrieve file: {error_msg}")
        print(error_msg)
        track_session_usage("get_json", False, error_msg)
@authorized
def diagnose_session(update, context):
    """Diagnose session issues"""
    try:
        # Check S3 file only
        s3_exists = file_exists_in_s3(f"sessions/{USER_SESSION_FILE}")
        
        msg = f"🔍 <b>Session Diagnosis (S3 ONLY)</b>\n\n"
        msg += f"📁 <b>Session File:</b> {USER_SESSION_FILE}\n"
        msg += f"☁️ <b>S3 Exists:</b> {'✅' if s3_exists else '❌'}\n\n"
        
        if not s3_exists:
            msg += "❌ <b>Problem:</b> No session file exists in S3!\n"
            msg += "💡 <b>Solution:</b> Run /setup_session to create session\n"
        else:
            msg += "🔧 <b>Problem:</b> Session exists but may not be authorized\n"
            msg += "💡 <b>Solution:</b> Run /check_session to verify\n"
        
        update.message.reply_text(msg, parse_mode="HTML")
        
    except Exception as e:
        update.message.reply_text(f"❌ Diagnosis error: {e}")

def start(update, context):
    user_id = update.effective_user.id
    if auth_collection.find_one({"user_id": user_id}):
        update.message.reply_text(
            "✅ You are already authorized!\n\n"
            "Available commands:\n"
            "/addchannel @ChannelUsername\n"
            "/listchannels\n"
            "/checkchannel @ChannelUsername\n"
            "/deletechannel @ChannelUsername\n"
            "/setup_session - Set up Telegram session (first time)\n"
            "/check_session - Check session status\n"
            "/checksessionusage - Session usage statistics\n"
            "/test - Test connection\n"
            "/check_data - Check scraped data\n"
            "/check_s3 - Check S3 status\n"
            "/cleanup - Cleanup sessions\n"
            "/clearhistory - Clear forwarded history\n"
            "/diagnose - Diagnose session health\n"
            "/debug_json - Debug S3 JSON file\n"
            "/debug_json - Debug a json file in S3\n"
            "/test_s3_write - Test S3 write access\n"
            "/debug_json_comprehensive - Comprehensive json debug"
        )
    else:
        update.message.reply_text(
            "⚡ Welcome! Please enter your access code using /code YOUR_CODE"
        )

def code(update, context):
    user_id = update.effective_user.id
    if auth_collection.find_one({"user_id": user_id}):
        update.message.reply_text("✅ You are already authorized!")
        return

    if len(context.args) == 0:
        update.message.reply_text("⚠️ Usage: /code YOUR_ACCESS_CODE")
        return

    entered_code = context.args[0].strip()
    if entered_code == ADMIN_CODE:
        auth_collection.insert_one({"user_id": user_id})
        update.message.reply_text(
            "✅ Code accepted! You can now use the bot commands.\n\n"
            "⚠️ Important: Run /setup_session first to set up your Telegram session."
        )
    else:
        update.message.reply_text("❌ Invalid code. Access denied.")

# ======================
# Main (S3 ONLY)
# ======================
def main():
    # Start Flask thread
    threading.Thread(target=run_flask, daemon=True).start()
    
    from telegram.utils.request import Request
    from telegram.error import Conflict
    
    # ✅ FIX: Create Bot with Request instead of passing to Updater
    request = Request(connect_timeout=30, read_timeout=30, con_pool_size=8)
    bot = Bot(token=BOT_TOKEN, request=request)
    
    # ✅ FIX: Pass bot to Updater instead of token
    updater = Updater(bot=bot, use_context=True)
    dp = updater.dispatcher
    
    # Add error handler
    dp.add_error_handler(error_handler)
   
    # All your existing handlers
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("code", code))
    dp.add_handler(CommandHandler("addchannel", add_channel))
    dp.add_handler(CommandHandler("listchannels", list_channels))
    dp.add_handler(CommandHandler("checkchannel", check_channel))
    dp.add_handler(CommandHandler("deletechannel", delete_channel))
    dp.add_handler(CommandHandler("setup_session", setup_session))
    dp.add_handler(CommandHandler("check_session", check_session))
    dp.add_handler(CommandHandler("checksessionusage", check_session_usage))
    dp.add_handler(CommandHandler("test", test_connection))
    dp.add_handler(CommandHandler("check_data", check_scraped_data))
    dp.add_handler(CommandHandler("check_s3", check_s3_status))
    dp.add_handler(CommandHandler("cleanup", cleanup_sessions))
    dp.add_handler(CommandHandler("clearhistory", clear_forwarded_history))
    dp.add_handler(CommandHandler("diagnose", diagnose_session))
    dp.add_handler(CommandHandler("debug_json", debug_s3_json))
    dp.add_handler(CommandHandler("deletes3files", delete_s3_files))
    dp.add_handler(CommandHandler("getscrapedjson", get_scraped_json))
    dp.add_handler(CommandHandler("debug_json_comprehensive", debug_json_comprehensive))
    dp.add_handler(MessageHandler(Filters.command, unknown_command))

    print(f"🤖 Bot is running...")
    print(f"🔧 Using session file: {USER_SESSION_FILE}")
    print(f"🌍 Environment: {'render' if 'RENDER' in os.environ else 'local'}")
    print(f"☁️ S3 Bucket: {AWS_BUCKET_NAME}")
    
    # Efficiently check all S3 files on startup
    print("\n🔍 Checking S3 files efficiently (using head_object)...")
    s3_status = check_s3_files_status()
    
    # Ensure S3 structure exists
    ensure_s3_structure()
    
    try:
        # ✅ FIX: Use improved start_polling with parameters
        updater.start_polling(
            timeout=10,
            drop_pending_updates=True,
            allowed_updates=['message', 'callback_query']
        )
        print("✅ Bot started successfully!")
        updater.idle()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down bot...")
    except Exception as e:
        print(f"❌ Bot error: {e}")
    finally:
        print("👋 Bot stopped")
if __name__ == "__main__":
    main()