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
import psutil
import speedtest
from telegram import Bot
from telegram.error import BadRequest, Conflict  
import schedule 
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler
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
def verified_channel_adder(update, context):
    """Add a channel and mark it as verified immediately"""
    if len(context.args) == 0:
        update.message.reply_text(
            "⚡ Usage: /verifiedchanneladder @ChannelUsername\n\n"
            "This will add the channel and mark it as verified immediately."
        )
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text("❌ Please provide a valid channel username starting with @")
        return

    # Check if channel already exists
    existing_channel = channels_collection.find_one({"username": username})
    if existing_channel:
        # Update existing channel to verified
        channels_collection.update_one(
            {"username": username},
            {"$set": {"isverified": True}}
        )
        update.message.reply_text(
            f"✅ <b>Channel verified successfully!</b>\n\n"
            f"📌 <b>Name:</b> {existing_channel.get('title', 'Unknown')}\n"
            f"🔗 <b>Username:</b> {username}\n"
            f"🟢 <b>Status:</b> Verified",
            parse_mode="HTML",
        )
    else:
        # Add new channel as verified
        try:
            chat = context.bot.get_chat(username)
            channels_collection.insert_one({
                "username": username, 
                "title": chat.title,
                "isverified": True,
                "verified_at": datetime.now(),
                "verified_by": update.effective_user.id
            })
            update.message.reply_text(
                f"✅ <b>Verified channel added successfully!</b>\n\n"
                f"📌 <b>Name:</b> {chat.title}\n"
                f"🔗 <b>Username:</b> {username}\n"
                f"🟢 <b>Status:</b> Verified",
                parse_mode="HTML",
            )
        except BadRequest as e:
            update.message.reply_text(f"❌ Could not add channel: {str(e)}")
            return
        except Exception as e:
            update.message.reply_text(f"❌ Unexpected error: {str(e)}")
            return

    # Run operations for verified channel
    def run_operations():
        try:
            # Forward messages
            update.message.reply_text(f"⏳ Forwarding last 7d posts from verified channel {username}...")
            success, result_msg = forward_last_7d_sync(username)
            context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
            
            # Add delay to ensure forwarding completes
            import time
            time.sleep(3)
            
            # Scrape data
            update.message.reply_text(f"⏳ Starting 7-day data scraping from verified channel {username}...")
            success, result_msg = scrape_channel_7days_sync(username)
            context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
            
        except Exception as e:
            error_msg = f"❌ Error during operations for verified channel: {str(e)}"
            context.bot.send_message(update.effective_chat.id, text=error_msg, parse_mode="HTML")

    threading.Thread(target=run_operations, daemon=True).start()
@authorized
def verify_channel(update, context):
    """Show available channels and allow toggling verified status with data cleanup"""
    if context.args:
        # Direct verification via command argument
        username = context.args[0].strip()
        if not username.startswith("@"):
            update.message.reply_text("❌ Please provide a valid channel username starting with @")
            return
        
        # Start verification process
        update.message.reply_text(f"🔄 Starting verification process for {username}...")
        threading.Thread(target=verify_channel_process, args=(update, context, username), daemon=True).start()
        return

    # Get all channels from database
    channels = list(channels_collection.find({}))
    
    if not channels:
        update.message.reply_text("📭 No channels saved yet. Use /addchannel first.")
        return

    # Create inline keyboard with channels
    keyboard = []
    for channel in channels:
        username = channel.get("username")
        title = channel.get("title", "Unknown")
        is_verified = channel.get("isverified", False)
        
        status_icon = "🟢" if is_verified else "🔴"
        button_text = f"{status_icon} {username} - {title}"
        
        # Truncate if too long
        if len(button_text) > 50:
            button_text = button_text[:47] + "..."
            
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"verify_{username}")])

    # Add a "Refresh" button
    keyboard.append([InlineKeyboardButton("🔄 Refresh List", callback_data="verify_refresh")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        "🔍 <b>Channel Verification Manager</b>\n\n"
        "🟢 = Verified | 🔴 = Not Verified\n\n"
        "Click on any channel to toggle its verified status:",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )
def verify_channel_process(update, context, username):
    """Process channel verification with data cleanup and refresh"""
    try:
        # Get the correct chat ID based on whether it's from a message or callback query
        if hasattr(update, 'effective_chat'):  # It's a Message update
            chat_id = update.effective_chat.id
            message_method = context.bot.send_message
        else:  # It's a CallbackQuery
            chat_id = update.message.chat.id
            message_method = context.bot.send_message
            # Answer the callback first
            update.answer()
        
        # Step 1: Clean up existing data for this channel
        message_method(chat_id, f"🧹 Step 1: Cleaning up existing data for {username}...")
        
        # Clean from forwarded messages
        all_forwarded_data = load_json_from_s3(f"data/{FORWARDED_FILE}")
        if isinstance(all_forwarded_data, dict) and username in all_forwarded_data:
            del all_forwarded_data[username]
            save_json_to_s3(all_forwarded_data, f"data/{FORWARDED_FILE}")
            message_method(chat_id, f"✅ Removed {username} from forwarded messages history")
        
        # Clean from scraped data
        scraped_data = load_scraped_data_from_s3()
        if scraped_data:
            initial_count = len(scraped_data)
            scraped_data = [item for item in scraped_data if item.get('channel') != username]
            removed_count = initial_count - len(scraped_data)
            if removed_count > 0:
                save_scraped_data_to_s3(scraped_data)
                message_method(chat_id, f"✅ Removed {removed_count} records of {username} from scraped data")
        
        # Step 2: Update channel verification status
        message_method(chat_id, f"🔄 Step 2: Updating verification status for {username}...")
        
        channel = channels_collection.find_one({"username": username})
        if channel:
            channels_collection.update_one(
                {"username": username},
                {"$set": {
                    "isverified": True,
                    "verified_at": datetime.now(),
                    "verified_by": update.effective_user.id if hasattr(update, 'effective_user') else update.message.from_user.id
                }}
            )
            message_method(chat_id, f"✅ Marked {username} as verified")
        else:
            # If channel doesn't exist in database, add it
            try:
                # For callback queries, we need to use the bot from context to get chat info
                if hasattr(update, 'effective_chat'):
                    chat = context.bot.get_chat(username)
                else:
                    chat = context.bot.get_chat(username)
                    
                channels_collection.insert_one({
                    "username": username, 
                    "title": chat.title,
                    "isverified": True,
                    "verified_at": datetime.now(),
                    "verified_by": update.effective_user.id if hasattr(update, 'effective_user') else update.message.from_user.id
                })
                message_method(chat_id, f"✅ Added and verified {username}")
            except Exception as e:
                message_method(chat_id, f"❌ Could not add channel: {str(e)}")
                return

        # Step 3: Forward last 7 days of messages
        message_method(chat_id, f"📤 Step 3: Forwarding last 7 days from {username}...")
        success, result_msg = forward_last_7d_sync(username)
        message_method(chat_id, text=result_msg, parse_mode="HTML")
        
        if not success and "No new posts" not in result_msg:
            message_method(chat_id, f"⚠️ Forwarding had issues, but continuing...")

        # Step 4: Scrape 7 days of data
        message_method(chat_id, f"🤖 Step 4: Scraping and AI-enhancing data from {username}...")
        success, result_msg = scrape_channel_7days_sync(username)
        message_method(chat_id, text=result_msg, parse_mode="HTML")

        # Final summary
        message_method(
            chat_id,
            f"✅ <b>Verification Complete!</b>\n\n"
            f"📌 <b>Channel:</b> {username}\n"
            f"🟢 <b>Status:</b> Verified\n"
            f"📊 <b>Data:</b> Cleaned and refreshed\n"
            f"🤖 <b>AI:</b> Enhanced with latest categories",
            parse_mode="HTML"
        )

    except Exception as e:
        error_msg = f"❌ Verification process failed: {str(e)}"
        message_method(chat_id, text=error_msg)
        print(f"❌ Verification error: {e}")
def verify_channel_callback(update, context):
    """Handle channel verification callback queries"""
    query = update.callback_query
    query.answer()
    
    callback_data = query.data
    
    if callback_data == "verify_refresh":
        # Refresh the channel list
        channels = list(channels_collection.find({}))
        keyboard = []
        for channel in channels:
            username = channel.get("username")
            title = channel.get("title", "Unknown")
            is_verified = channel.get("isverified", False)
            
            status_icon = "🟢" if is_verified else "🔴"
            button_text = f"{status_icon} {username} - {title}"
            
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
                
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"verify_{username}")])
        
        keyboard.append([InlineKeyboardButton("🔄 Refresh List", callback_data="verify_refresh")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            "🔍 <b>Channel Verification Manager</b>\n\n"
            "🟢 = Verified | 🔴 = Not Verified\n\n"
            "Click on any channel to toggle its verified status:",
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
        return
    
    if callback_data.startswith("verify_"):
        username = callback_data[7:]  # Remove "verify_" prefix
        
        # Start FULL verification process in background (not just toggle status)
        query.edit_message_text(f"🔄 Starting FULL verification process for {username}...")
        threading.Thread(target=verify_channel_process, args=(query, context, username), daemon=True).start()

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
# 7-day scraping function with EXACT 1:1 matching
# ======================
async def scrape_channel_7days_async(channel_username: str):
    """Scrape last 7 days of data with exact 1:1 target channel matching"""
    telethon_client = None
    
    try:
        print("🔍 Loading json data directly from S3...")
        
        telethon_client = await get_telethon_client()
        if not telethon_client:
            track_session_usage("scraping", False, "Failed to initialize client")
            return False, "❌ Could not establish connection for scraping."
        
        print(f"🔍 Starting 7-day scrape for channel: {channel_username}")
        channel_data = channels_collection.find_one({"username": channel_username})
        is_verified = channel_data.get("isverified", False) if channel_data else False
        
        
        try:
            # Get the SOURCE channel entity (where we scrape FROM)
            source_entity = await telethon_client.get_entity(channel_username)
            print(f"✅ Source channel found: {source_entity.title}")
            
            # Get the TARGET channel entity (where messages are forwarded TO)
            target_entity = await telethon_client.get_entity(FORWARD_CHANNEL)
            print(f"✅ Target channel found: {target_entity.title}")
            
        except (ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            track_session_usage("scraping", False, f"Invalid channel: {str(e)}")
            return False, f"❌ Channel {channel_username} is invalid or doesn't exist."

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        print(f"⏰ Scraping ALL messages from last 7 days (since {cutoff})")

        # STEP 1: Collect source messages (PROTECTED with cutoff)
        source_messages = []
        source_message_count = 0
        
        print(f"📡 Collecting source messages from: {channel_username}")
        
        async for message in telethon_client.iter_messages(source_entity, limit=None):
            source_message_count += 1
            if source_message_count % 20 == 0:
                print(f"📊 Scanned {source_message_count} source messages... Found {len(source_messages)} valid")
                
            # 🔥 PROTECTED CUTOFF: Stop when we reach 7-day limit
            if message.date < cutoff:
                print(f"⏹️ Reached 7-day cutoff in source. Scanned {source_message_count} total, found {len(source_messages)} valid")
                break
                
            if not message.text:
                continue

            source_messages.append({
                'text': message.text,
                'date': message.date,
                'source_channel': channel_username,
                'source_message_id': message.id
            })

        # STEP 2: Scan target channel for matches (PROTECTED with cutoff)
        scraped_data = []
        target_message_count = 0
        matched_count = 0
        used_source_ids = set()  # 🔥 TRACK which source messages we've already matched
        used_target_ids = set()  # 🔥 TRACK which target messages we've already matched
        
        print(f"🔍 Scanning target channel for forwarded messages...")
        
        async for target_message in telethon_client.iter_messages(target_entity, limit=None):
            target_message_count += 1
            if target_message_count % 20 == 0:
                print(f"📊 Scanned {target_message_count} target messages... Found {matched_count} matches")
                
            # 🔥 PROTECTED CUTOFF: Stop when we reach 7-day limit in target
            if target_message.date < cutoff:
                print(f"⏹️ Reached 7-day cutoff in target. Scanned {target_message_count} total, found {matched_count} matches")
                break
                
            if not target_message.text:
                continue
                
            # 🔥 PREVENT DUPLICATE TARGET MATCHES
            if target_message.id in used_target_ids:
                continue

            # Find matching source message by text content
            matching_source = None
            for source_msg in source_messages:
                # 🔥 PREVENT DUPLICATE SOURCE MATCHES
                if source_msg['source_message_id'] in used_source_ids:
                    continue
                    
                source_text_clean = source_msg['text'].strip().lower()
                target_text_clean = target_message.text.strip().lower()
                
                # Strategy 1: Exact text match (most reliable)
                if source_text_clean == target_text_clean:
                    matching_source = source_msg
                    break
                
                # Strategy 2: Source text contained in target (for forwarded messages with added text)
                if source_text_clean in target_text_clean:
                    matching_source = source_msg
                    break
                    
                # Strategy 3: Target text contained in source (for truncated forwards)
                if target_text_clean in source_text_clean:
                    matching_source = source_msg
                    break

                # Strategy 4: First 100 characters match (for very similar posts)
                if (len(source_text_clean) > 100 and len(target_text_clean) > 100 and
                    source_text_clean[:100] == target_text_clean[:100]):
                    matching_source = source_msg
                    break

            if not matching_source:
                continue

            matched_count += 1
            # 🔥 MARK BOTH SOURCE AND TARGET AS USED to prevent duplicates
            used_source_ids.add(matching_source['source_message_id'])
            used_target_ids.add(target_message.id)

            # Extract info from TARGET message text
            info = extract_info(target_message.text, target_message.id)
            
            # AI ENHANCEMENT
            print(f"🤖 AI enriching matched message {target_message.id}: {info['title'][:50]}...")
            predicted_category, generated_description = enrich_product_with_ai(info["title"], info["description"])            
            
            # 🔥 CORRECT: Use ACTUAL target message ID for post_link
            if getattr(target_entity, "username", None):
                post_link = f"https://t.me/{target_entity.username}/{target_message.id}"
            else:
                internal_id = str(target_entity.id)
                if internal_id.startswith("-100"):
                    internal_id = internal_id[4:]
                post_link = f"https://t.me/c/{internal_id}/{target_message.id}"

            product_ref = str(target_message.id)  # Use actual target message ID

            # Create data structure
            post_data = {
                "title": info["title"],
                "description": info["description"],
                "price": info["price"],
                "phone": info["phone"],
                "location": info["location"],
                "date": target_message.date.strftime("%Y-%m-%d %H:%M:%S"),
                "channel": channel_username,  # Keep source channel name
                "post_link": post_link,       # Link to ACTUAL target message
                "product_ref": product_ref,   # ACTUAL target message ID
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "predicted_category": predicted_category,
                "generated_description": generated_description,
                "ai_enhanced": True,
                "has_media": bool(target_message.media),
                "has_text": bool(target_message.text),
                "source_message_id": matching_source['source_message_id'],  # Original source ID
                "target_message_id": target_message.id,   # Actual target channel ID
                "channel_verified": is_verified
            }
            scraped_data.append(post_data)
        
        print(f"📋 Found {len(scraped_data)} exact 1:1 matches with AI enhancement")
        print(f"📊 Statistics: {len(source_messages)} source messages, {matched_count} matched in target channel")
        
        # Load existing data DIRECTLY FROM S3 JSON
        existing_data = load_scraped_data_from_s3()
        
        print(f"📁 Loaded existing data with {len(existing_data)} records from S3 JSON")

        if scraped_data:
            # Combine and deduplicate by product_ref (actual target message ID)
            combined_data = existing_data.copy()
            
            # Create a set of existing product_refs for quick lookup
            existing_refs = {item['product_ref'] for item in existing_data}
            
            # Add new items that don't exist
            new_items_added = 0
            for new_item in scraped_data:
                if new_item['product_ref'] not in existing_refs:
                    combined_data.append(new_item)
                    new_items_added += 1
            
            # Save ONLY to S3 as JSON
            success = save_scraped_data_to_s3(combined_data)
            if success:
                print(f"💾 Saved {len(combined_data)} total records to S3 with AI enhancement")
                
                # Print AI enhancement summary
                from collections import Counter
                category_counts = Counter(item['predicted_category'] for item in scraped_data)
                print("🤖 AI Enhancement Summary:")
                for category, count in category_counts.most_common(5):
                    print(f"  • {category}: {count} products")
                
                track_session_usage("scraping", True, f"Scraped {len(scraped_data)} messages with AI")
                
                result_msg = f"✅ Scraped {len(scraped_data)} messages from {channel_username}. "
                result_msg += f"Added {new_items_added} new AI-enhanced records to database. "
                result_msg += f"(All linked to {FORWARD_CHANNEL})"
                
                return True, result_msg
            else:
                track_session_usage("scraping", False, "S3 save failed")
                return False, f"❌ Failed to save AI-enhanced data for {channel_username} to S3."
        else:
            track_session_usage("scraping", True, "No new messages found")
            return False, f"📭 No messages found for {channel_username} in the last 7 days."
            
    except Exception as e:
        error_msg = f"Scraping error: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        track_session_usage("scraping", False, error_msg)
        return False, f"❌ Scraping error: {str(e)}"
    finally:
        if telethon_client:
            try:
                await telethon_client.disconnect()
                # Upload session file to S3 after operations
                print("📤 Uploading updated session file to S3...")
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
                print(f"⚠️ Error during cleanup: {e}")

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
def add_multiple_channels(update, context):
    """Add multiple channels at once separated by commas"""
    if len(context.args) == 0:
        update.message.reply_text(
            "⚡ Usage: /addmultiplechannels @channel1,@channel2,@channel3\n\n"
            "Example: /addmultiplechannels @channel1,@channel2,@channel3"
        )
        return

    channels_input = ' '.join(context.args)
    channel_usernames = [username.strip() for username in channels_input.split(',') if username.strip()]
    
    if not channel_usernames:
        update.message.reply_text("❌ Please provide valid channel usernames separated by commas.")
        return

    # Validate usernames
    invalid_usernames = []
    valid_usernames = []
    
    for username in channel_usernames:
        if not username.startswith("@"):
            invalid_usernames.append(username)
        else:
            valid_usernames.append(username)

    if invalid_usernames:
        update.message.reply_text(
            f"❌ Invalid usernames (must start with @): {', '.join(invalid_usernames)}"
        )
        return

    # Check for duplicates
    existing_channels = []
    new_channels = []
    
    for username in valid_usernames:
        if channels_collection.find_one({"username": username}):
            existing_channels.append(username)
        else:
            new_channels.append(username)

    if not new_channels:
        update.message.reply_text(
            f"⚠️ All channels already exist in database: {', '.join(existing_channels)}"
        )
        return

    # Add new channels
    added_channels = []
    failed_channels = []
    
    for username in new_channels:
        try:
            chat = context.bot.get_chat(username)
            channels_collection.insert_one({"username": username, "title": chat.title})
            added_channels.append(f"{username} — {chat.title}")
        except BadRequest as e:
            failed_channels.append(f"{username} — {str(e)}")
        except Exception as e:
            failed_channels.append(f"{username} — {str(e)}")

    # Send results
    result_message = "📊 <b>Multiple Channels Add Results</b>\n\n"
    
    if added_channels:
        result_message += f"✅ <b>Successfully Added ({len(added_channels)}):</b>\n"
        for channel in added_channels:
            result_message += f"• {channel}\n"
        result_message += "\n"
    
    if existing_channels:
        result_message += f"⚠️ <b>Already Existed ({len(existing_channels)}):</b>\n"
        for channel in existing_channels:
            result_message += f"• {channel}\n"
        result_message += "\n"
    
    if failed_channels:
        result_message += f"❌ <b>Failed to Add ({len(failed_channels)}):</b>\n"
        for channel in failed_channels:
            result_message += f"• {channel}\n"

    update.message.reply_text(result_message, parse_mode="HTML")

    # Run operations for successfully added channels
    if added_channels:
        def run_operations_for_channels():
            try:
                for username in new_channels:
                    if any(username in added for added in added_channels):
                        # Forward messages
                        update.message.reply_text(f"⏳ Forwarding last 7d posts from {username}...")
                        success, result_msg = forward_last_7d_sync(username)
                        context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
                        
                        time.sleep(2)
                        
                        # Scrape data
                        update.message.reply_text(f"⏳ Starting 7-day data scraping from {username}...")
                        success, result_msg = scrape_channel_7days_sync(username)
                        context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
                        
                        time.sleep(1)  # Small delay between channels
                        
            except Exception as e:
                error_msg = f"❌ Error during operations: {str(e)}"
                context.bot.send_message(update.effective_chat.id, text=error_msg, parse_mode="HTML")

        threading.Thread(target=run_operations_for_channels, daemon=True).start()
@authorized
def optimization_checker(update, context):
    """Comprehensive optimization checker for Render, AWS, and Hugging Face API"""
    
    def run_optimization_check():
        try:
            # Start with initial message
            progress_msg = context.bot.send_message(
                update.effective_chat.id, 
                "🔍 Starting comprehensive optimization check...\n\n⏳ This may take 1-2 minutes"
            )
            
            results = {
                "system": {},
                "aws_s3": {},
                "huggingface": {},
                "telegram": {},
                "performance": {},
                "recommendations": []
            }
            
            # 1. System Performance Check
            try:
                # CPU Usage
                cpu_percent = psutil.cpu_percent(interval=1)
                results["system"]["cpu_usage"] = f"{cpu_percent}%"
                
                # Memory Usage
                memory = psutil.virtual_memory()
                results["system"]["memory_usage"] = f"{memory.percent}%"
                results["system"]["memory_available"] = f"{memory.available / (1024**3):.1f} GB"
                
                # Disk Usage
                disk = psutil.disk_usage('/')
                results["system"]["disk_usage"] = f"{disk.percent}%"
                results["system"]["disk_free"] = f"{disk.free / (1024**3):.1f} GB"
                
            except Exception as e:
                results["system"]["error"] = f"System check failed: {str(e)}"
            
            # 2. AWS S3 Performance Check
            try:
                s3_start_time = time.time()
                
                # Test S3 connection speed
                test_key = "performance_test.txt"
                test_content = "x" * 1024  # 1KB test file
                
                # Upload test
                upload_start = time.time()
                s3.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=test_key,
                    Body=test_content.encode('utf-8')
                )
                upload_time = time.time() - upload_start
                
                # Download test
                download_start = time.time()
                response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=test_key)
                response['Body'].read()
                download_time = time.time() - download_start
                
                # Cleanup
                s3.delete_object(Bucket=AWS_BUCKET_NAME, Key=test_key)
                
                results["aws_s3"]["upload_speed"] = f"{(1024 / upload_time / 1024):.2f} MB/s"
                results["aws_s3"]["download_speed"] = f"{(1024 / download_time / 1024):.2f} MB/s"
                results["aws_s3"]["total_s3_time"] = f"{(time.time() - s3_start_time):.2f}s"
                
                # Check S3 file sizes
                try:
                    scraped_data_size = 0
                    if file_exists_in_s3(f"data/{SCRAPED_DATA_FILE}"):
                        response = s3.head_object(Bucket=AWS_BUCKET_NAME, Key=f"data/{SCRAPED_DATA_FILE}")
                        scraped_data_size = response['ContentLength'] / (1024**2)  # MB
                    
                    results["aws_s3"]["scraped_data_size"] = f"{scraped_data_size:.2f} MB"
                    
                except Exception as e:
                    results["aws_s3"]["size_check_error"] = str(e)
                    
            except Exception as e:
                results["aws_s3"]["error"] = f"S3 check failed: {str(e)}"
            
            # 3. Hugging Face API Performance Check
            try:
                hf_start_time = time.time()
                
                # Test classification API
                test_text = "This is a test product description for optimization checking."
                test_categories = [{"name": "Electronics"}, {"name": "Fashion"}, {"name": "Other"}]
                
                payload = {
                    "inputs": test_text,
                    "parameters": {"candidate_labels": [cat["name"] for cat in test_categories]}
                }
                
                hf_response = query_huggingface_api(payload, "facebook/bart-large-mnli")
                hf_time = time.time() - hf_start_time
                
                if hf_response:
                    results["huggingface"]["api_response_time"] = f"{hf_time:.2f}s"
                    results["huggingface"]["api_status"] = "✅ Working"
                    
                    if hf_time > 5:
                        results["recommendations"].append("🤖 Hugging Face API is slow (>5s). Consider caching results.")
                else:
                    results["huggingface"]["api_status"] = "❌ Failed"
                    results["huggingface"]["api_response_time"] = f"{hf_time:.2f}s"
                    
            except Exception as e:
                results["huggingface"]["error"] = f"HF API check failed: {str(e)}"
            
            # 4. Telegram API Performance Check
            try:
                tg_start_time = time.time()
                
                async def test_telegram_async():
                    client = await get_telethon_client()
                    if client:
                        me = await client.get_me()
                        await client.disconnect()
                        return True, f"Connected as {me.first_name}"
                    return False, "Failed to connect"
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                tg_success, tg_message = loop.run_until_complete(test_telegram_async())
                loop.close()
                
                tg_time = time.time() - tg_start_time
                
                results["telegram"]["connection_time"] = f"{tg_time:.2f}s"
                results["telegram"]["status"] = "✅ " + tg_message if tg_success else "❌ " + tg_message
                
            except Exception as e:
                results["telegram"]["error"] = f"Telegram check failed: {str(e)}"
            
            # 5. Network Speed Test (if speedtest-cli is available)
            try:
                st = speedtest.Speedtest()
                st.get_best_server()
                
                download_speed = st.download() / 1024 / 1024  # Convert to Mbps
                upload_speed = st.upload() / 1024 / 1024  # Convert to Mbps
                
                results["performance"]["download_speed"] = f"{download_speed:.2f} Mbps"
                results["performance"]["upload_speed"] = f"{upload_speed:.2f} Mbps"
                
                if download_speed < 10:
                    results["recommendations"].append("🌐 Network download speed is slow (<10 Mbps). May affect performance.")
                if upload_speed < 5:
                    results["recommendations"].append("🌐 Network upload speed is slow (<5 Mbps). May affect S3 operations.")
                    
            except Exception as e:
                results["performance"]["speedtest_error"] = f"Speed test failed: {str(e)}"
            
            # 6. Database Performance Check
            try:
                db_start_time = time.time()
                
                # Check channels count
                channels_count = channels_collection.count_documents({})
                
                # Check scraped data count
                scraped_data = load_scraped_data_from_s3()
                scraped_count = len(scraped_data) if scraped_data else 0
                
                # Check session usage stats
                session_stats = get_session_usage_stats()
                
                db_time = time.time() - db_start_time
                
                results["performance"]["database_query_time"] = f"{db_time:.2f}s"
                results["performance"]["channels_count"] = channels_count
                results["performance"]["scraped_records"] = scraped_count
                results["performance"]["session_operations_24h"] = session_stats.get('total_operations', 0) if session_stats else 0
                
                if db_time > 2:
                    results["recommendations"].append("💾 Database queries are slow (>2s). Consider indexing.")
                    
            except Exception as e:
                results["performance"]["database_error"] = f"Database check failed: {str(e)}"
            
            # Generate final report
            report = generate_optimization_report(results)
            
            # Update the progress message with final report
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=progress_msg.message_id,
                text=report,
                parse_mode="HTML"
            )
            
        except Exception as e:
            error_msg = f"❌ Optimization check failed: {str(e)}"
            context.bot.send_message(update.effective_chat.id, text=error_msg)

    threading.Thread(target=run_optimization_check, daemon=True).start()

def generate_optimization_report(results):
    """Generate a formatted optimization report"""
    report = "📊 <b>Comprehensive Optimization Report</b>\n\n"
    
    # System Performance
    report += "🖥️ <b>System Performance:</b>\n"
    for key, value in results.get("system", {}).items():
        report += f"• {key.replace('_', ' ').title()}: {value}\n"
    report += "\n"
    
    # AWS S3 Performance
    report += "☁️ <b>AWS S3 Performance:</b>\n"
    for key, value in results.get("aws_s3", {}).items():
        report += f"• {key.replace('_', ' ').title()}: {value}\n"
    report += "\n"
    
    # Hugging Face API
    report += "🤖 <b>Hugging Face API:</b>\n"
    for key, value in results.get("huggingface", {}).items():
        report += f"• {key.replace('_', ' ').title()}: {value}\n"
    report += "\n"
    
    # Telegram API
    report += "📱 <b>Telegram API:</b>\n"
    for key, value in results.get("telegram", {}).items():
        report += f"• {key.replace('_', ' ').title()}: {value}\n"
    report += "\n"
    
    # Network Performance
    report += "🌐 <b>Network Performance:</b>\n"
    for key, value in results.get("performance", {}).items():
        if "error" not in key:
            report += f"• {key.replace('_', ' ').title()}: {value}\n"
    report += "\n"
    
    # Recommendations
    if results.get("recommendations"):
        report += "💡 <b>Optimization Recommendations:</b>\n"
        for i, recommendation in enumerate(results["recommendations"], 1):
            report += f"{i}. {recommendation}\n"
    else:
        report += "✅ <b>No major optimization issues detected!</b>\n"
    
    # Overall Status
    report += f"\n⏰ <b>Report Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    report += f"🔧 <b>Environment:</b> {'🚀 Render' if 'RENDER' in os.environ else '💻 Local'}\n"
    
    return report
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
        is_verified = ch.get("isverified", False)
        status_icon = "🟢" if is_verified else "🔴"
        msg_lines.append(f"{status_icon} {username} — <b>{title}</b>")

    msg = "\n".join(msg_lines)
    for chunk in [msg[i : i + 4000] for i in range(0, len(msg), 4000)]:
        update.message.reply_text(chunk, parse_mode="HTML")

# Add this function to get only verified channels for your updates
def get_verified_channels():
    """Get all verified channels"""
    return list(channels_collection.find({"isverified": True}))

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
def start_24h_auto_scraping(update, context):
    """Start automatic 24-hour scraping and forwarding (Bot2 functionality)"""
    def run_auto_scraping():
        try:
            async def auto_scraping_async():
                telethon_client = None
                try:
                    update.message.reply_text("🔄 Starting 24-hour auto-scraping cycle...")
                    
                    # Step 1: Forward messages from all channels
                    update.message.reply_text("📤 Step 1: Forwarding new messages from all channels...")
                    
                    telethon_client = await get_telethon_client()
                    if not telethon_client:
                        return False, "❌ Could not establish connection."
                    
                    # Get all channels from database with their verification status
                    channels = list(channels_collection.find({}))
                    if not channels:
                        return False, "❌ No channels found in database."
                    
                    # Create a dictionary for quick verification status lookup
                    channel_verification_status = {}
                    for ch in channels:
                        channel_verification_status[ch["username"]] = ch.get("isverified", False)
                    
                    channel_usernames = [ch["username"] for ch in channels]
                    
                    now = datetime.now(timezone.utc)
                    cutoff = now - timedelta(days=1)  # 24 hours
                    
                    # Load forwarded messages from S3
                    all_forwarded_data = load_json_from_s3(f"data/{FORWARDED_FILE}")
                    if not isinstance(all_forwarded_data, dict):
                        all_forwarded_data = {}
                    
                    total_forwarded = 0
                    
                    for channel_username in channel_usernames:
                        try:
                            update.message.reply_text(f"⏳ Processing {channel_username}...")
                            
                            entity = await telethon_client.get_entity(channel_username)
                            print(f"✅ Channel found: {entity.title}")
                            
                            # Get or initialize for this channel
                            channel_forwarded = all_forwarded_data.get(channel_username, {})
                            forwarded_ids = {
                                int(msg_id): datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") 
                                for msg_id, ts in channel_forwarded.items()
                            } if channel_forwarded else {}
                            
                            # Remove old forwarded IDs
                            week_cutoff = now - timedelta(days=7)
                            forwarded_ids = {msg_id: ts for msg_id, ts in forwarded_ids.items() 
                                           if ts >= week_cutoff.replace(tzinfo=None)}
                            
                            messages_to_forward = []
                            
                            async for message in telethon_client.iter_messages(entity, limit=None):
                                if message.date < cutoff:
                                    break
                                    
                                if message.id not in forwarded_ids and (message.text or message.media):
                                    messages_to_forward.append(message)
                            
                            if not messages_to_forward:
                                continue
                            
                            # Reverse to forward in chronological order
                            messages_to_forward.reverse()
                            channel_forwarded_count = 0
                            
                            # Forward in batches
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
                                    
                                    for msg in batch:
                                        forwarded_ids[msg.id] = msg.date.replace(tzinfo=None)
                                        channel_forwarded_count += 1
                                        total_forwarded += 1
                                    
                                    await asyncio.sleep(1)
                                    
                                except ChatForwardsRestrictedError:
                                    print(f"🚫 Forwarding restricted for {channel_username}")
                                    break
                                except FloodWaitError as e:
                                    await asyncio.sleep(e.seconds)
                                    continue
                                except Exception as e:
                                    print(f"⚠️ Error forwarding batch: {e}")
                                    continue
                            
                            # Update channel data
                            all_forwarded_data[channel_username] = {
                                str(k): v.strftime("%Y-%m-%d %H:%M:%S") for k, v in forwarded_ids.items()
                            }
                            
                            if channel_forwarded_count > 0:
                                update.message.reply_text(f"✅ Forwarded {channel_forwarded_count} messages from {channel_username}")
                            
                        except Exception as e:
                            print(f"❌ Error processing {channel_username}: {e}")
                            continue
                    
                    # Save all forwarded data
                    save_json_to_s3(all_forwarded_data, f"data/{FORWARDED_FILE}")
                    
                    # Step 2: Scrape and AI-enrich from target channel
                    update.message.reply_text("🤖 Step 2: Scraping and AI-enriching from target channel...")
                    
                    try:
                        target_entity = await telethon_client.get_entity(FORWARD_CHANNEL)
                        print(f"✅ Target channel: {target_entity.title}")
                        
                        # Collect source messages from all channels with verification status
                        source_messages = []
                        for channel_username in channel_usernames:
                            try:
                                source_entity = await telethon_client.get_entity(channel_username)
                                is_verified = channel_verification_status.get(channel_username, False)
                                
                                async for message in telethon_client.iter_messages(source_entity, limit=None):
                                    if not message.text:
                                        continue
                                    if message.date < cutoff:
                                        break
                                    source_messages.append({
                                        'text': message.text,
                                        'date': message.date,
                                        'source_channel': channel_username,
                                        'source_message_id': message.id,
                                        'is_verified': is_verified  # Include verification status here
                                    })
                            except Exception as e:
                                print(f"❌ Error collecting from {channel_username}: {e}")
                                continue
                        
                        # Load existing data to check for updates
                        existing_data = load_scraped_data_from_s3()
                        
                        # 🔥 FIX: Create a unique identifier that combines channel + source_message_id
                        # This prevents conflicts between different channels
                        existing_unique_refs = {
                            f"{item['channel']}_{item['source_message_id']}" 
                            for item in existing_data 
                            if 'channel' in item and 'source_message_id' in item
                        }
                        
                        # Scan target channel for matches
                        scraped_data = []
                        used_source_ids = set()
                        used_target_ids = set()
                        updated_items = 0
                        new_items = 0
                        
                        async for target_message in telethon_client.iter_messages(target_entity, limit=None):
                            if not target_message.text:
                                continue
                            if target_message.date < cutoff:
                                break
                            if target_message.id in used_target_ids:
                                continue
                            
                            # Find matching source message
                            matching_source = None
                            for source_msg in source_messages:
                                if source_msg['source_message_id'] in used_source_ids:
                                    continue
                                    
                                source_text_clean = source_msg['text'].strip().lower()
                                target_text_clean = target_message.text.strip().lower()
                                
                                if (source_text_clean == target_text_clean or
                                    source_text_clean in target_text_clean or
                                    target_text_clean in source_text_clean or
                                    (len(source_text_clean) > 100 and len(target_text_clean) > 100 and
                                     source_text_clean[:100] == target_text_clean[:100])):
                                    matching_source = source_msg
                                    break
                            
                            if not matching_source:
                                continue
                            
                            used_source_ids.add(matching_source['source_message_id'])
                            used_target_ids.add(target_message.id)
                            
                            # Extract info and AI enhance
                            info = extract_info(target_message.text, target_message.id)
                            predicted_category, generated_description = enrich_product_with_ai(info["title"], info["description"])
                            
                            # Create post link
                            if getattr(target_entity, "username", None):
                                post_link = f"https://t.me/{target_entity.username}/{target_message.id}"
                            else:
                                internal_id = str(target_entity.id)
                                if internal_id.startswith("-100"):
                                    internal_id = internal_id[4:]
                                post_link = f"https://t.me/c/{internal_id}/{target_message.id}"
                            
                            # Use the verification status from the source message
                            is_verified = matching_source.get('is_verified', False)
                            
                            # 🔥 FIX: Create unique reference combining channel and source message ID
                            unique_ref = f"{matching_source['source_channel']}_{matching_source['source_message_id']}"
                            is_new_item = unique_ref not in existing_unique_refs
                            
                            post_data = {
                                "title": info["title"],
                                "description": info["description"],
                                "price": info["price"],
                                "phone": info["phone"],
                                "location": info["location"],
                                "date": target_message.date.strftime("%Y-%m-%d %H:%M:%S"),
                                "channel": matching_source['source_channel'],
                                "post_link": post_link,
                                "product_ref": str(target_message.id),  # Keep original for compatibility
                                "unique_ref": unique_ref,  # 🔥 NEW: Unique identifier
                                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "predicted_category": predicted_category,
                                "generated_description": generated_description,
                                "ai_enhanced": True,
                                "source_message_id": matching_source['source_message_id'],
                                "target_message_id": target_message.id,
                                "channel_verified": is_verified  # This should now be correctly set
                            }
                            
                            if is_new_item:
                                new_items += 1
                                scraped_data.append(post_data)
                                print(f"🆕 New item found: {unique_ref}")
                            else:
                                # Update existing item with new AI enhancement and verification status
                                updated_items += 1
                                # Find and update the existing item using unique_ref
                                for i, existing_item in enumerate(existing_data):
                                    if existing_item.get('unique_ref') == unique_ref:
                                        # Update AI-enhanced fields and verification status
                                        existing_data[i].update({
                                            "predicted_category": predicted_category,
                                            "generated_description": generated_description,
                                            "ai_enhanced": True,
                                            "channel_verified": is_verified,
                                            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            "post_link": post_link,  # Update post link in case it changed
                                            "target_message_id": target_message.id  # Update target message ID
                                        })
                                        print(f"🔄 Updated existing item: {unique_ref}")
                                        break
                        
                        # Combine new items with updated existing data
                        if scraped_data or updated_items > 0:
                            # For new items, add to existing data
                            if scraped_data:
                                combined_data = existing_data + scraped_data
                            else:
                                combined_data = existing_data
                            
                            success = save_scraped_data_to_s3(combined_data)
                            
                            if success:
                                # Print comprehensive summary
                                category_counts = {}
                                verified_count = 0
                                total_processed = len(scraped_data) + updated_items
                                
                                # Count categories for new items
                                for item in scraped_data:
                                    category = item.get('predicted_category', 'Unknown')
                                    category_counts[category] = category_counts.get(category, 0) + 1
                                    if item.get('channel_verified', False):
                                        verified_count += 1
                                
                                # Count categories for updated items (approximate)
                                for item in existing_data[:updated_items]:  # Just sample some updated items
                                    category = item.get('predicted_category', 'Unknown')
                                    category_counts[category] = category_counts.get(category, 0) + 1
                                    if item.get('channel_verified', False):
                                        verified_count += 1
                                
                                summary_msg = f"✅ 24-hour cycle completed!\n\n"
                                summary_msg += f"📤 Forwarded: {total_forwarded} messages\n"
                                summary_msg += f"🤖 AI Processing:\n"
                                summary_msg += f"   • New items: {new_items}\n"
                                summary_msg += f"   • Updated items: {updated_items}\n"
                                summary_msg += f"   • From verified channels: {verified_count}\n"
                                summary_msg += f"   • Total processed: {total_processed}\n\n"
                                
                                if category_counts:
                                    summary_msg += f"📊 Categories:\n"
                                    for category, count in list(category_counts.items())[:5]:  # Top 5 categories
                                        summary_msg += f"• {category}: {count}\n"
                                
                                return True, summary_msg
                            else:
                                return False, "❌ Failed to save AI-enhanced data to S3."
                        else:
                            return True, f"✅ 24-hour cycle completed!\n\n📤 Forwarded: {total_forwarded} messages\n🤖 No new or updated products to AI-enhance."
                            
                    except Exception as e:
                        return False, f"❌ Scraping error: {str(e)}"
                    
                except Exception as e:
                    return False, f"❌ Auto-scraping error: {str(e)}"
                finally:
                    if telethon_client:
                        await telethon_client.disconnect()
                        # Upload session to S3
                        if os.path.exists(USER_SESSION_FILE):
                            with open(USER_SESSION_FILE, 'rb') as f:
                                s3.put_object(
                                    Bucket=AWS_BUCKET_NAME,
                                    Key=f"sessions/{USER_SESSION_FILE}",
                                    Body=f.read()
                                )
                            os.remove(USER_SESSION_FILE)
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            success, result = loop.run_until_complete(auto_scraping_async())
            loop.close()
            
            context.bot.send_message(update.effective_chat.id, text=result)
            
        except Exception as e:
            context.bot.send_message(update.effective_chat.id, text=f"❌ Error in auto-scraping: {e}")
    
    threading.Thread(target=run_auto_scraping, daemon=True).start()
    update.message.reply_text("🚀 Starting 24-hour auto-scraping cycle in background...")

@authorized
def remove_all_verified(update, context):
    """Remove verified status from all channels"""
    # Confirm action
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, remove all", callback_data="bulk_remove_all_verified"),
            InlineKeyboardButton("❌ Cancel", callback_data="bulk_remove_cancel")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        "⚠️ <b>Are you sure you want to remove verified status from ALL channels?</b>\n\n"
        "This action cannot be undone!",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

def bulk_remove_callback(update, context):
    """Handle bulk remove verified callback queries"""
    query = update.callback_query
    query.answer()
    
    callback_data = query.data
    
    if callback_data == "bulk_remove_all_verified":
        # Remove verified status from all channels
        result = channels_collection.update_many(
            {"isverified": True},
            {"$set": {
                "isverified": False,
                "verified_at": None,
                "verified_by": None
            }}
        )
        
        query.edit_message_text(
            f"✅ <b>Verified status removed from all channels!</b>\n\n"
            f"📊 <b>Channels affected:</b> {result.modified_count}\n\n"
            f"All channels are now unverified.",
            parse_mode="HTML"
        )
        
    elif callback_data == "bulk_remove_cancel":
        query.edit_message_text("❌ Operation cancelled. No changes were made.")

@authorized
def remove_verified(update, context):
    """Remove verified status from channels with data cleanup - works exactly like verifychannel but opposite"""
    if context.args:
        # Direct removal via command argument
        username = context.args[0].strip()
        if not username.startswith("@"):
            update.message.reply_text("❌ Please provide a valid channel username starting with @")
            return
        
        # Start removal process
        update.message.reply_text(f"🔄 Starting removal process for {username}...")
        threading.Thread(target=remove_verified_process, args=(update, context, username), daemon=True).start()
        return

    # Get all channels from database (show both verified and unverified like verifychannel)
    channels = list(channels_collection.find({}))
    
    if not channels:
        update.message.reply_text("📭 No channels saved yet. Use /addchannel first.")
        return

    # Create inline keyboard with ALL channels (like verifychannel), but highlight verified ones
    keyboard = []
    for channel in channels:
        username = channel.get("username")
        title = channel.get("title", "Unknown")
        is_verified = channel.get("isverified", False)
        
        # Show verified channels with red X, unverified with gray circle
        status_icon = "❌" if is_verified else "⚪"
        button_text = f"{status_icon} {username} - {title}"
        
        # Truncate if too long
        if len(button_text) > 50:
            button_text = button_text[:47] + "..."
            
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"remove_verify_{username}")])

    # Add a "Refresh" button (like verifychannel)
    keyboard.append([InlineKeyboardButton("🔄 Refresh List", callback_data="remove_verified_refresh")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        "🔴 <b>Remove Verification Manager</b>\n\n"
        "❌ = Verified (can remove) | ⚪ = Not Verified (already unverified)\n\n"
        "Click on any channel to remove its verified status (full cleanup + refresh):",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )
def remove_verified_process(update, context, username):
    """Process verified status removal with data cleanup and refresh"""
    try:
        # Get the correct chat ID based on whether it's from a message or callback query
        if hasattr(update, 'effective_chat'):  # It's a Message update
            chat_id = update.effective_chat.id
            message_method = context.bot.send_message
        else:  # It's a CallbackQuery
            chat_id = update.message.chat.id
            message_method = context.bot.send_message
            # Answer the callback first
            update.answer()
        
        # Step 1: Clean up existing data for this channel
        message_method(chat_id, f"🧹 Step 1: Cleaning up existing data for {username}...")
        
        # Clean from forwarded messages
        all_forwarded_data = load_json_from_s3(f"data/{FORWARDED_FILE}")
        if isinstance(all_forwarded_data, dict) and username in all_forwarded_data:
            del all_forwarded_data[username]
            save_json_to_s3(all_forwarded_data, f"data/{FORWARDED_FILE}")
            message_method(chat_id, f"✅ Removed {username} from forwarded messages history")
        
        # Clean from scraped data
        scraped_data = load_scraped_data_from_s3()
        if scraped_data:
            initial_count = len(scraped_data)
            scraped_data = [item for item in scraped_data if item.get('channel') != username]
            removed_count = initial_count - len(scraped_data)
            if removed_count > 0:
                save_scraped_data_to_s3(scraped_data)
                message_method(chat_id, f"✅ Removed {removed_count} records of {username} from scraped data")
        
        # Step 2: Update channel verification status
        message_method(chat_id, f"🔄 Step 2: Removing verified status from {username}...")
        
        channel = channels_collection.find_one({"username": username})
        if channel:
            channels_collection.update_one(
                {"username": username},
                {"$set": {
                    "isverified": False,
                    "verified_at": None,
                    "verified_by": None
                }}
            )
            message_method(chat_id, f"✅ Removed verified status from {username}")
        else:
            message_method(chat_id, f"❌ Channel {username} not found in database")
            return

        # Step 3: Forward last 7 days of messages (as unverified)
        message_method(chat_id, f"📤 Step 3: Forwarding last 7 days from {username} (unverified)...")
        success, result_msg = forward_last_7d_sync(username)
        message_method(chat_id, text=result_msg, parse_mode="HTML")
        
        if not success and "No new posts" not in result_msg:
            message_method(chat_id, f"⚠️ Forwarding had issues, but continuing...")

        # Step 4: Scrape 7 days of data (as unverified)
        message_method(chat_id, f"🤖 Step 4: Scraping data from {username} (unverified)...")
        success, result_msg = scrape_channel_7days_sync(username)
        message_method(chat_id, text=result_msg, parse_mode="HTML")

        # Final summary
        message_method(
            chat_id,
            f"✅ <b>Verification Removal Complete!</b>\n\n"
            f"📌 <b>Channel:</b> {username}\n"
            f"🔴 <b>Status:</b> Not Verified\n"
            f"📊 <b>Data:</b> Cleaned and refreshed\n"
            f"🔄 <b>Updated:</b> All data marked as unverified",
            parse_mode="HTML"
        )

    except Exception as e:
        error_msg = f"❌ Verification removal process failed: {str(e)}"
        message_method(chat_id, text=error_msg)
        print(f"❌ Verification removal error: {e}")
def remove_verified_callback(update, context):
    """Handle remove verified callback queries"""
    query = update.callback_query
    query.answer()
    
    callback_data = query.data
    
    if callback_data == "remove_verified_refresh":
        # Refresh the verified channels list
        verified_channels = list(channels_collection.find({"isverified": True}))
        
        if not verified_channels:
            query.edit_message_text("ℹ️ No verified channels found.")
            return
        
        keyboard = []
        for channel in verified_channels:
            username = channel.get("username")
            title = channel.get("title", "Unknown")
            button_text = f"🔴 {username} - {title}"
            
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
                
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"remove_verify_{username}")])
        
        keyboard.append([InlineKeyboardButton("🔄 Refresh List", callback_data="remove_verified_refresh")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            "🔴 <b>Remove Verified Status</b>\n\n"
            "Click on any channel to remove its verified status and clean its data:",
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
        return
    
    if callback_data.startswith("remove_verify_"):
        username = callback_data[14:]  # Remove "remove_verify_" prefix
        
        # Start removal process in background
        query.edit_message_text(f"🔄 Starting removal process for {username}...")
        threading.Thread(target=remove_verified_process, args=(query, context, username), daemon=True).start()
@authorized
def clean_up7day(update, context):
    """Remove data older than 7 days from scraped data and forwarded messages for all channels"""
    def run_cleanup():
        try:
            context.bot.send_message(update.effective_chat.id, "🧹 Starting 7-day data cleanup for all channels...")
            
            # Calculate cutoff date (7 days ago)
            cutoff_date = datetime.now() - timedelta(days=7)
            context.bot.send_message(update.effective_chat.id, f"⏰ Removing data older than: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Step 1: Clean up scraped data
            context.bot.send_message(update.effective_chat.id, "📊 Step 1: Cleaning up scraped data...")
            scraped_cleanup_result = clean_up_scraped_data_7days(cutoff_date)
            context.bot.send_message(update.effective_chat.id, scraped_cleanup_result)
            
            # Step 2: Clean up forwarded messages
            context.bot.send_message(update.effective_chat.id, "📨 Step 2: Cleaning up forwarded messages history...")
            forwarded_cleanup_result = clean_up_forwarded_messages_7days(cutoff_date)
            context.bot.send_message(update.effective_chat.id, forwarded_cleanup_result)
            
            # Final summary
            context.bot.send_message(
                update.effective_chat.id,
                f"✅ <b>7-Day Cleanup Complete!</b>\n\n"
                f"📊 <b>Scraped Data:</b> {scraped_cleanup_result}\n"
                f"📨 <b>Forwarded Messages:</b> {forwarded_cleanup_result}\n\n"
                f"🧹 All data older than 7 days has been removed.",
                parse_mode="HTML"
            )
            
        except Exception as e:
            error_msg = f"❌ Cleanup process failed: {str(e)}"
            context.bot.send_message(update.effective_chat.id, text=error_msg)
            print(f"❌ Cleanup error: {e}")
    
    threading.Thread(target=run_cleanup, daemon=True).start()

def clean_up_scraped_data_7days(cutoff_date):
    """Remove scraped data older than 7 days"""
    try:
        # Load current scraped data
        scraped_data = load_scraped_data_from_s3()
        if not scraped_data:
            return "No scraped data found to clean"
        
        initial_count = len(scraped_data)
        
        # Filter out records older than 7 days
        cleaned_data = []
        removed_count = 0
        
        for item in scraped_data:
            try:
                # Parse the date from the item
                if 'date' in item and item['date']:
                    item_date = datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S")
                    if item_date >= cutoff_date:
                        cleaned_data.append(item)
                    else:
                        removed_count += 1
                else:
                    # Keep items without date (shouldn't happen, but just in case)
                    cleaned_data.append(item)
            except Exception as e:
                # If date parsing fails, keep the item
                print(f"⚠️ Error parsing date for item: {item.get('product_ref', 'unknown')}, error: {e}")
                cleaned_data.append(item)
        
        # Save cleaned data
        if save_scraped_data_to_s3(cleaned_data):
            return f"Removed {removed_count} old records, kept {len(cleaned_data)} recent records"
        else:
            return f"Failed to save cleaned data after removing {removed_count} records"
            
    except Exception as e:
        return f"Error cleaning scraped data: {str(e)}"

def clean_up_forwarded_messages_7days(cutoff_date):
    """Remove forwarded messages history older than 7 days"""
    try:
        # Load current forwarded messages data
        all_forwarded_data = load_json_from_s3(f"data/{FORWARDED_FILE}")
        if not isinstance(all_forwarded_data, dict):
            return "No forwarded messages history found to clean"
        
        initial_total_messages = sum(len(messages) for messages in all_forwarded_data.values())
        removed_total = 0
        channels_affected = 0
        
        # Clean each channel's forwarded messages
        for channel_username, channel_messages in all_forwarded_data.items():
            if not isinstance(channel_messages, dict):
                continue
                
            initial_channel_count = len(channel_messages)
            cleaned_messages = {}
            
            for message_id_str, message_date_str in channel_messages.items():
                try:
                    message_date = datetime.strptime(message_date_str, "%Y-%m-%d %H:%M:%S")
                    if message_date >= cutoff_date:
                        cleaned_messages[message_id_str] = message_date_str
                    else:
                        removed_total += 1
                except Exception as e:
                    # If date parsing fails, keep the message
                    print(f"⚠️ Error parsing date for message {message_id_str} in {channel_username}, error: {e}")
                    cleaned_messages[message_id_str] = message_date_str
            
            # Update channel data
            all_forwarded_data[channel_username] = cleaned_messages
            if len(cleaned_messages) < initial_channel_count:
                channels_affected += 1
        
        # Save cleaned forwarded messages data
        if save_json_to_s3(all_forwarded_data, f"data/{FORWARDED_FILE}"):
            final_total_messages = sum(len(messages) for messages in all_forwarded_data.values())
            return f"Removed {removed_total} old messages from {channels_affected} channels, kept {final_total_messages} recent messages"
        else:
            return f"Failed to save cleaned forwarded messages data after removing {removed_total} messages"
            
    except Exception as e:
        return f"Error cleaning forwarded messages: {str(e)}"
@authorized
def verification_status(update, context):
    """Enhanced verification management with more options"""
    # Get verification statistics
    total_channels = channels_collection.count_documents({})
    verified_channels = channels_collection.count_documents({"isverified": True})
    unverified_channels = total_channels - verified_channels
    
    keyboard = [
        [InlineKeyboardButton("📊 Verification Statscomprhensive", callback_data="verification_stats")],
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        f"🔧 <b>Verification Manager</b>\n\n"
        f"📊 <b>Current Status:</b>\n"
        f"• Total Channels: {total_channels}\n"
        f"• Verified: {verified_channels}\n"
        f"• Not Verified: {unverified_channels}\n\n"
        f"Choose an action:",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

def verification_status_callback(update, context):
    """Handle verification manager callbacks"""
    query = update.callback_query
    query.answer()
    
    callback_data = query.data
    
    print(f"🔍 Callback received: {callback_data}")  # Debug log
    
    if callback_data == "verify_channel_list":
        # Show list of unverified channels to verify
        unverified_channels = list(channels_collection.find({"isverified": False}))
        
        if not unverified_channels:
            query.edit_message_text("ℹ️ All channels are already verified.")
            return
        
        keyboard = []
        for channel in unverified_channels:
            username = channel.get("username")
            title = channel.get("title", "Unknown")
            button_text = f"🔴 {username} - {title}"
            
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
                
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"verify_{username}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="verification_status_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            "🟢 <b>Verify Channels</b>\n\n"
            "Click on a channel to mark it as verified:",
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
    
    elif callback_data == "remove_verified_list":
        # Show list of verified channels to remove
        verified_channels = list(channels_collection.find({"isverified": True}))
        
        if not verified_channels:
            query.edit_message_text("ℹ️ No verified channels found.")
            return
        
        keyboard = []
        for channel in verified_channels:
            username = channel.get("username")
            title = channel.get("title", "Unknown")
            button_text = f"🔴 {username} - {title}"
            
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
                
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"remove_verify_{username}")])
        
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            "🔴 <b>Remove Verified Status</b>\n\n"
            "Click on a channel to remove its verified status:",
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
    
    elif callback_data.startswith("verify_"):
        # Handle both verification callbacks (from verify_channel and verification_status)
        if callback_data == "verify_refresh":
            # Handle refresh from the main verify_channel command
            verify_channel_callback(update, context)
            return
            
        username = callback_data[7:]  # Remove "verify_" prefix
        
        # Start FULL verification process in background
        query.edit_message_text(f"🔄 Starting FULL verification process for {username}...")
        threading.Thread(target=verify_channel_process, args=(query, context, username), daemon=True).start()
    
    elif callback_data.startswith("remove_verify_"):
        username = callback_data[14:]  # Remove "remove_verify_" prefix
        
        # Find the channel
        channel = channels_collection.find_one({"username": username})
        if not channel:
            query.edit_message_text(f"❌ Channel {username} not found in database.")
            return
        
        # Remove verified status
        channels_collection.update_one(
            {"username": username},
            {"$set": {
                "isverified": False,
                "verified_at": None,
                "verified_by": None
            }}
        )
        
        query.edit_message_text(
            f"✅ <b>Verified status removed!</b>\n\n"
            f"📌 <b>Channel:</b> {channel.get('title', 'Unknown')}\n"
            f"🔗 <b>Username:</b> {username}\n"
            f"🔴 <b>Status:</b> Not Verified\n\n"
            f"Use /verificationstatus for more options.",
            parse_mode="HTML"
        )
    
    elif callback_data == "verification_stats":
        # Show detailed statistics
        total_channels = channels_collection.count_documents({})
        verified_channels = channels_collection.count_documents({"isverified": True})
        unverified_channels = total_channels - verified_channels
        
        # Get recently verified channels
        recent_verified = list(channels_collection.find(
            {"isverified": True, "verified_at": {"$ne": None}}
        ).sort("verified_at", -1).limit(5))
        
        # Get recently unverified channels
        recent_unverified = list(channels_collection.find(
            {"isverified": False, "verified_at": {"$ne": None}}
        ).sort("verified_at", -1).limit(5))
        
        stats_msg = f"📊 <b>Verification Statistics</b>\n\n"
        stats_msg += f"• Total Channels: {total_channels}\n"
        stats_msg += f"• Verified: {verified_channels}\n"
        stats_msg += f"• Not Verified: {unverified_channels}\n"
        stats_msg += f"• Verification Rate: {(verified_channels/total_channels*100) if total_channels > 0 else 0:.1f}%\n\n"
        
        if recent_verified:
            stats_msg += f"🕒 <b>Recently Verified:</b>\n"
            for channel in recent_verified:
                verified_time = channel.get('verified_at', datetime.now()).strftime("%Y-%m-%d %H:%M")
                stats_msg += f"• {channel.get('username')} - {verified_time}\n"
            stats_msg += "\n"
        
        if recent_unverified:
            stats_msg += f"🕒 <b>Recently Unverified:</b>\n"
            for channel in recent_unverified:
                unverified_time = channel.get('verified_at', datetime.now()).strftime("%Y-%m-%d %H:%M")
                stats_msg += f"• {channel.get('username')} - {unverified_time}\n"
        
        query.edit_message_text(stats_msg, parse_mode="HTML")
    
    elif callback_data == "remove_all_verified_prompt":
        # Confirm bulk removal
        keyboard = [
            [
                InlineKeyboardButton("✅ Yes, remove all", callback_data="remove_all_verified_confirm"),
                InlineKeyboardButton("❌ Cancel", callback_data="verification_status_back")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        verified_count = channels_collection.count_documents({"isverified": True})
        
        query.edit_message_text(
            f"⚠️ <b>Confirm Bulk Removal</b>\n\n"
            f"You are about to remove verified status from <b>{verified_count} channels</b>.\n\n"
            f"❌ This action cannot be undone!\n\n"
            f"Are you sure?",
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
    
    elif callback_data == "remove_all_verified_confirm":
        # Remove verified status from all channels
        result = channels_collection.update_many(
            {"isverified": True},
            {"$set": {
                "isverified": False,
                "verified_at": None,
                "verified_by": None
            }}
        )
        
        query.edit_message_text(
            f"✅ <b>Verified status removed from all channels!</b>\n\n"
            f"📊 <b>Channels affected:</b> {result.modified_count}\n\n"
            f"All channels are now unverified.",
            parse_mode="HTML"
        )
    
    elif callback_data == "verification_status_back":
        # Go back to main manager
        verification_status(update, context)
    
    else:
        # Handle unknown callback data
        query.edit_message_text(f"❌ Unknown callback: {callback_data}")@authorized
def check_verification_status(update, context):
    """Check verification status of channels and their data"""
    try:
        # Get all channels from MongoDB
        channels = list(channels_collection.find({}))
        
        # Get scraped data from S3
        scraped_data = load_scraped_data_from_s3()
        
        msg = "🔍 <b>Channel Verification Status Report</b>\n\n"
        
        msg += "<b>MongoDB Channels:</b>\n"
        verified_count = 0
        for channel in channels:
            username = channel.get("username")
            is_verified = channel.get("isverified", False)
            status = "🟢 Verified" if is_verified else "🔴 Not Verified"
            if is_verified:
                verified_count += 1
            msg += f"• {username}: {status}\n"
        
        msg += f"\n<b>Summary:</b> {verified_count}/{len(channels)} channels verified\n\n"
        
        if scraped_data:
            # Count verified channels in scraped data
            verified_in_json = sum(1 for item in scraped_data if item.get('channel_verified', False))
            total_in_json = len(scraped_data)
            
            msg += f"<b>Scraped Data:</b>\n"
            msg += f"• Total records: {total_in_json}\n"
            msg += f"• From verified channels: {verified_in_json}\n"
            msg += f"• From unverified channels: {total_in_json - verified_in_json}\n"
        else:
            msg += "<b>Scraped Data:</b> No data found\n"
        
        update.message.reply_text(msg, parse_mode="HTML")
        
    except Exception as e:
        update.message.reply_text(f"❌ Error checking verification status: {e}")
@authorized
def multiple_verifiedchanneladder(update, context):
    """Add multiple channels and mark them as verified immediately"""
    if len(context.args) == 0:
        update.message.reply_text(
            "⚡ Usage: /multipleverifiedchanneladder @channel1,@channel2,@channel3\n\n"
            "This will add multiple channels and mark them as verified immediately.\n\n"
            "Example: /multipleverifiedchanneladder @channel1,@channel2,@channel3"
        )
        return

    channels_input = ' '.join(context.args)
    channel_usernames = [username.strip() for username in channels_input.split(',') if username.strip()]
    
    if not channel_usernames:
        update.message.reply_text("❌ Please provide valid channel usernames separated by commas.")
        return

    # Validate usernames
    invalid_usernames = []
    valid_usernames = []
    
    for username in channel_usernames:
        if not username.startswith("@"):
            invalid_usernames.append(username)
        else:
            valid_usernames.append(username)

    if invalid_usernames:
        update.message.reply_text(
            f"❌ Invalid usernames (must start with @): {', '.join(invalid_usernames)}"
        )
        return

    # Check for duplicates and process channels
    existing_channels = []
    new_channels = []
    failed_channels = []
    added_channels = []
    
    for username in valid_usernames:
        try:
            # Check if channel already exists
            existing_channel = channels_collection.find_one({"username": username})
            if existing_channel:
                # Update existing channel to verified
                channels_collection.update_one(
                    {"username": username},
                    {"$set": {
                        "isverified": True,
                        "verified_at": datetime.now(),
                        "verified_by": update.effective_user.id
                    }}
                )
                existing_channels.append(f"{username} — {existing_channel.get('title', 'Unknown')}")
            else:
                # Add new channel as verified
                chat = context.bot.get_chat(username)
                channels_collection.insert_one({
                    "username": username, 
                    "title": chat.title,
                    "isverified": True,
                    "verified_at": datetime.now(),
                    "verified_by": update.effective_user.id
                })
                new_channels.append(f"{username} — {chat.title}")
                added_channels.append(username)
                
        except BadRequest as e:
            failed_channels.append(f"{username} — {str(e)}")
        except Exception as e:
            failed_channels.append(f"{username} — {str(e)}")

    # Send results
    result_message = "📊 <b>Multiple Verified Channels Add Results</b>\n\n"
    
    if new_channels:
        result_message += f"✅ <b>New Verified Channels Added ({len(new_channels)}):</b>\n"
        for channel in new_channels:
            result_message += f"• {channel}\n"
        result_message += "\n"
    
    if existing_channels:
        result_message += f"🔄 <b>Existing Channels Updated to Verified ({len(existing_channels)}):</b>\n"
        for channel in existing_channels:
            result_message += f"• {channel}\n"
        result_message += "\n"
    
    if failed_channels:
        result_message += f"❌ <b>Failed to Add/Verify ({len(failed_channels)}):</b>\n"
        for channel in failed_channels:
            result_message += f"• {channel}\n"

    update.message.reply_text(result_message, parse_mode="HTML")

    # Run operations for successfully added channels
    if added_channels:
        def run_operations_for_channels():
            try:
                for username in added_channels:
                    try:
                        # Forward messages
                        update.message.reply_text(f"⏳ Forwarding last 7d posts from verified channel {username}...")
                        success, result_msg = forward_last_7d_sync(username)
                        context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
                        
                        time.sleep(2)
                        
                        # Scrape data
                        update.message.reply_text(f"⏳ Starting 7-day data scraping from verified channel {username}...")
                        success, result_msg = scrape_channel_7days_sync(username)
                        context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
                        
                        time.sleep(1)  # Small delay between channels
                        
                    except Exception as e:
                        error_msg = f"❌ Error during operations for {username}: {str(e)}"
                        context.bot.send_message(update.effective_chat.id, text=error_msg, parse_mode="HTML")
                        continue
                        
            except Exception as e:
                error_msg = f"❌ Error during operations: {str(e)}"
                context.bot.send_message(update.effective_chat.id, text=error_msg, parse_mode="HTML")

        threading.Thread(target=run_operations_for_channels, daemon=True).start()
@authorized  
def schedule_24h_auto_scraping(update, context):
    """Schedule the 24-hour auto-scraping to run daily"""
    def run_scheduler():
        import time
        
        def execute_daily_task():
            try:
                # This would need to be adapted to run the auto-scraping logic
                # For now, we'll just send a notification
                bot = Bot(token=BOT_TOKEN)
                bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="🕒 Daily auto-scraping time! Use /start_24h_auto_scraping to run now."
                )
            except Exception as e:
                print(f"❌ Scheduler error: {e}")
        
        # Schedule daily at a specific time (e.g., 8:00 AM)
        schedule.every().day.at("08:00").do(execute_daily_task)
        
        update.message.reply_text(
            "✅ 24-hour auto-scraping scheduled!\n\n"
            "🕒 Will run daily at 08:00 AM\n"
            "🔔 You will receive a notification when it's time to run.\n\n"
            "💡 Use /start_24h_auto_scraping to run immediately."
        )
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)
            except Exception as e:
                print(f"❌ Scheduler error: {e}")
                time.sleep(60)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()


@authorized
def unknown_command(update, context):
    update.message.reply_text(
        "❌ *Unknown command\.*\n\n"
        "📋 *Available Commands:*\n\n"
        "🔧 *Channel Management:*\n"
        "┣ /addchannel @ChannelUsername\n"
        "┣ /addmultiplechannels @channel1,@channel2,\.\.\.\n"
        "┣ /multipleverifiedchanneladder @channel1,@channel2,\.\.\.\n"
        "┣ /verifiedchanneladder @ChannelUsername\n"
        "┣ /listchannels\n"
        "┣ /checkchannel @ChannelUsername\n"
        "┣ /deletechannel @ChannelUsername\n\n"
        
        "⚡ *Session & System:*\n"
        "┣ /check_session - Check session status\n"
        "┣ /checksessionusage \- Session usage stats\n"
        "┣ /optimizationchecker \- Comprehensive performance check\n"
        "┣ /test \- Test connection\n\n"
        
        "📊 *Data Management:*\n"
        "┣ /check_data \- Check scraped data\n"
        "┣ /check_s3 \- Check S3 status\n"
        "┣ /diagnose \- Diagnose session health\n"
        "┣ /debug_json \- Debug S3 JSON file\n"
        "┣ /deletes3files \- Delete S3 files\n"
        "┣ /getscrapedjson \- Get JSON file\n\n"
        "┣ /clean_up7day \- Remove data older than 7 days\n\n" 
        "🤖 *Automation:*\n"
        "┣ /start_24h_auto_scraping \- Run 24\-hour auto\-scraping cycle\n"
        # "┣ /schedule\_24h\_auto\_scraping \- Schedule daily auto\-scraping\n\n"
        
        "🛡️ *Verification Management:*\n"
        "┣ /verifychannel \- Verify channel access\n"
        "┣ /verificationstatus \- Manage verification status\n"
        "┣ /removeverified \- Show verified channels to remove verification\n"
        "┣ /removeverified @ChannelUsername \- Remove verified status\n"
        # "┣ /removeallverified \- Remove all verified status\n", 
        "🛡️version:2\n"

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
   "✅ *You are already authorized\!*\n\n"
            "📋 *Available Commands:*\n\n"
            "🔧 *Channel Management:*\n"
            "┣ /addchannel @ChannelUsername\n"
            "┣ /addmultiplechannels @channel1,@channel2,\.\.\.\n"
            "┣ /multipleverifiedchanneladder @channel1,@channel2,\.\.\.\n"
            "┣ /verifiedchanneladder @ChannelUsername\n"
            "┣ /listchannels\n"
            "┣ /checkchannel @ChannelUsername\n"
            "┣ /deletechannel @ChannelUsername\n\n"
            
            "⚡ *Session & System:*\n"
            "┣ /check_session \- Check session status\n"
            "┣ /checksessionusage \- Session usage stats\n"
            "┣ /optimizationchecker \- Comprehensive performance check\n"
            "┣ /test \- Test connection\n\n"
            
            "📊 *Data Management:*\n"
            "┣ /check_data \- Check scraped data\n"
            "┣ /check_s3 \- Check S3 status\n"
            "┣ /debug_json \- Debug S3 JSON file\n"
            "┣ /deletes3files \- Delete S3 files\n"
            "┣ /getscrapedjson \- Get JSON file\n\n"
            "┣ /clean_up7day \- Remove data older than 7 days\n\n" 
            "🤖 *Automation:*\n"
            "┣ /start_24h_auto_scraping \- Run 24\-hour auto\-scraping cycle\n"
            # "┣ /schedule_24h_auto_scraping \- Schedule daily auto\-scraping\n\n"
            
            "🛡️ *Verification Management:*\n"
            "┣ /verifychannel \- Verify channel access\n"
            "┣ /verificationstatus \- Manage verification status\n"
            "┣ /removeverified \- Show verified channels to remove\n"
            "┣ /removeverified @ChannelUsername \- Remove verified status\n"
            "┣ /removeallverified \- Remove all verified status\n\n"
            
            "🔍 *Troubleshooting:*\n"
            "┣ /diagnose \- Diagnose session health\n"
            "┣ /debug_json_comprehensive \- Detailed JSON debug\n",
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
    dp.add_handler(CommandHandler("addmultiplechannels", add_multiple_channels))
    dp.add_handler(CommandHandler("multipleverifiedchanneladder", multiple_verifiedchanneladder))
    dp.add_handler(CommandHandler("verifiedchanneladder", verified_channel_adder))
    dp.add_handler(CommandHandler("verifychannel", verify_channel))
   
    dp.add_handler(CommandHandler("optimizationchecker", optimization_checker))
    dp.add_handler(CommandHandler("listchannels", list_channels))
    dp.add_handler(CommandHandler("checkchannel", check_channel))
    dp.add_handler(CommandHandler("deletechannel", delete_channel))
    # dp.add_handler(CommandHandler("setup_session", setup_session))
    dp.add_handler(CommandHandler("check_session", check_session))
    dp.add_handler(CommandHandler("checksessionusage", check_session_usage))
    dp.add_handler(CommandHandler("test", test_connection))
    dp.add_handler(CommandHandler("check_data", check_scraped_data))
    dp.add_handler(CommandHandler("check_s3", check_s3_status))
    # dp.add_handler(CommandHandler("cleanup", cleanup_sessions))
    # dp.add_handler(CommandHandler("clearhistory", clear_forwarded_history))
    dp.add_handler(CommandHandler("diagnose", diagnose_session))
    dp.add_handler(CommandHandler("debug_json", debug_s3_json))
    dp.add_handler(CommandHandler("deletes3files", delete_s3_files))
    dp.add_handler(CommandHandler("getscrapedjson", get_scraped_json))
    dp.add_handler(CommandHandler("debug_json_comprehensive", debug_json_comprehensive))
    dp.add_handler(CommandHandler("start_24h_auto_scraping", start_24h_auto_scraping))
    dp.add_handler(CommandHandler("schedule_24h_auto_scraping", schedule_24h_auto_scraping))
    dp.add_handler(CommandHandler("checkverification", check_verification_status))
    dp.add_handler(CommandHandler("removeverified", remove_verified))
    dp.add_handler(CommandHandler("removeallverified", remove_all_verified))
    dp.add_handler(CommandHandler("verificationstatus", verification_status))
    dp.add_handler(CommandHandler("clean_up7day", clean_up7day))
    dp.add_handler(CallbackQueryHandler(remove_verified_callback, pattern="^remove_all_verified_"))
    dp.add_handler(CallbackQueryHandler(verification_status_callback, pattern="^verification_"))
    dp.add_handler(CallbackQueryHandler(verification_status_callback, pattern="^remove_verify_"))
    dp.add_handler(CallbackQueryHandler(remove_verified_callback, pattern="^remove_verify_|remove_verified_refresh"))
    dp.add_handler(CallbackQueryHandler(verification_status_callback, pattern="^(verify_|remove_verify_|verification_stats|remove_all_verified|verification_status)"))
    dp.add_handler(CallbackQueryHandler(remove_verified_callback, pattern="^remove_all_verified_"))
    dp.add_handler(CallbackQueryHandler(verify_channel_callback, pattern="^verify_"))
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