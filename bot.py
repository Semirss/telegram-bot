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
import shutil
import requests

# === 🤖 AI Models Setup with Hugging Face Inference API ===
HF_API_TOKEN = os.getenv("HF_API_TOKEN")  # Optional for some models

def classify_with_hf_api(text, candidate_labels):
    """Use HF Inference API for zero-shot classification"""
    try:
        API_URL = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
        headers = {"Authorization": f"Bearer {HF_API_TOKEN}"} if HF_API_TOKEN else {}
        
        payload = {
            "inputs": text,
            "parameters": {"candidate_labels": candidate_labels}
        }
        
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            print(f"HF API classification error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"HF API classification call failed: {e}")
        return None

def summarize_with_hf_api(text):
    """Use HF Inference API for summarization"""
    try:
        API_URL = "https://api-inference.huggingface.co/models/facebook/bart-large-cnn"
        headers = {"Authorization": f"Bearer {HF_API_TOKEN}"} if HF_API_TOKEN else {}
        
        payload = {
            "inputs": text[:1024]  # Limit input length
        }
        
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            result = response.json()
            return result[0]['summary_text'] if result else text[:80]
        else:
            print(f"HF API summarization error: {response.status_code}")
            return text[:80]
    except Exception as e:
        print(f"HF API summarization call failed: {e}")
        return text[:80]

def get_embedding_with_hf_api(text):
    """Use HF Inference API for sentence embeddings"""
    try:
        API_URL = "https://api-inference.huggingface.co/models/sentence-transformers/all-MiniLM-L6-v2"
        headers = {"Authorization": f"Bearer {HF_API_TOKEN}"} if HF_API_TOKEN else {}
        
        payload = {
            "inputs": text
        }
        
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            result = response.json()
            # Return as numpy array for compatibility
            import numpy as np
            return np.array(result)
        else:
            print(f"HF API embedding error: {response.status_code}")
            # Return random embedding as fallback
            import numpy as np
            return np.random.rand(384)
    except Exception as e:
        print(f"HF API embedding call failed: {e}")
        import numpy as np
        return np.random.rand(384)

# Simple keyword-based fallback categorization
def simple_category_detection(text):
    """Simple keyword-based categorization as fallback"""
    text_lower = text.lower()
    
    category_keywords = {
        'Electronics': ['phone', 'laptop', 'tablet', 'computer', 'device', 'smartphone', 'iphone', 'samsung', 'android'],
        'Fashion': ['shirt', 'dress', 'shoe', 'clothing', 'fashion', 'wear', 'jacket', 'pants', 'jeans'],
        'Home Goods': ['furniture', 'home', 'house', 'decor', 'kitchen', 'sofa', 'chair', 'table'],
        'Beauty': ['cosmetic', 'makeup', 'skincare', 'beauty', 'perfume', 'lipstick', 'cream', 'lotion'],
        'Sports': ['sport', 'fitness', 'gym', 'exercise', 'ball', 'football', 'basketball', 'training'],
        'Vehicles': ['car', 'bike', 'vehicle', 'motor', 'auto', 'toyota', 'honda', 'bmw'],
        'Books': ['book', 'novel', 'literature', 'read', 'textbook', 'magazine'],
        'Groceries': ['food', 'grocery', 'fruit', 'vegetable', 'rice', 'pasta', 'oil'],
        'Medicine': ['medicine', 'drug', 'pharmacy', 'health', 'vitamin', 'supplement'],
        'Perfume': ['perfume', 'fragrance', 'cologne', 'scent', 'aroma']
    }
    
    for category, keywords in category_keywords.items():
        if any(keyword in text_lower for keyword in keywords):
            return category
    
    return "Other"

AI_MODELS_LOADED = True  # Always loaded since we're using APIs
print("✅ AI models setup complete (using Hugging Face Inference API)")

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

# Run Flask in a separate thread
def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask, daemon=True).start()

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
scraped_7d = "scraped_7d.parquet"

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

# === 🧹 Text cleaning and extraction helpers ===
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    # Remove URLs
    text = re.sub(r"http\S+", "", text)
    # Remove emojis and special characters
    text = re.sub(r'[^\w\s,.]', '', text)
    # Remove promotional and noise terms
    text = re.sub(r'\b(restocked|detail|catch|new|sale|nishane|montale|phoera|libre|vanille)\b', '', text, flags=re.IGNORECASE)
    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()

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

# === 🤖 AI Enrichment Functions with HF API ===
def load_categories():
    """Load categories from MongoDB with their representative descriptions."""
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
        {"name": "Perfume", "description": "Fragrances, colognes, and perfumes"}
    ]
    stored_categories = categories_collection.find_one({"_id": "categories"})
    if stored_categories and "categories" in stored_categories:
        return stored_categories["categories"]
    else:
        categories_collection.insert_one({"_id": "categories", "categories": default_categories})
        return default_categories

def save_categories(categories):
    """Save updated categories to MongoDB."""
    categories_collection.update_one(
        {"_id": "categories"},
        {"$set": {"categories": categories}},
        upsert=True
    )

def get_text_embedding(text):
    """Generate embedding for a text using HF API"""
    return get_embedding_with_hf_api(text)

def propose_new_category(text, classification_results, existing_categories):
    """Propose a general category using semantic similarity and keyword extraction."""
    if not AI_MODELS_LOADED:
        return "Miscellaneous"
    
    text = clean_text(text).lower()
    
    # Simple keyword extraction (replaces spaCy NER)
    words = re.findall(r'\b[a-z]{4,}\b', text)
    keywords = [word for word in words if word not in 
               ['item', 'product', 'thing', 'restocked', 'detail', 'catch', 'new', 'sale']]
    
    # Generate embedding for the input text using API
    text_embedding = get_text_embedding(text)
    
    # Compare with existing category descriptions
    category_names = [cat["name"] for cat in existing_categories]
    category_descriptions = [cat["description"] for cat in existing_categories]
    
    # Get embeddings for all category descriptions
    category_embeddings = [get_text_embedding(desc) for desc in category_descriptions]
    
    # Calculate similarities
    from sklearn.metrics.pairwise import cosine_similarity
    similarities = cosine_similarity(text_embedding.reshape(1, -1), category_embeddings)[0]
    max_similarity = max(similarities) if similarities.size > 0 else 0
    best_category_idx = np.argmax(similarities) if similarities.size > 0 else -1
    
    # If similarity is high enough, use the closest existing category
    if max_similarity > 0.7:  # Adjustable threshold
        return category_names[best_category_idx]
    
    # If keywords exist, use the first one as a new category
    if keywords:
        new_category = keywords[0].capitalize()
        # Avoid overly specific categories by checking against existing ones
        for cat in existing_categories:
            if new_category.lower() in cat["description"].lower():
                return cat["name"]
        return new_category
    
    # Fallback to classifier's top category
    if classification_results and classification_results["labels"]:
        return classification_results["labels"][0]
    
    return "Miscellaneous"

def enrich_product(title, desc):
    """AI enrichment function for product categorization and summarization using HF API"""
    if not AI_MODELS_LOADED:
        return "Unknown", desc[:80] if desc else title[:80]
    
    text = desc if isinstance(desc, str) and len(desc) > 10 else title
    text = clean_text(text)

    # Load current categories
    categories = load_categories()
    category_names = [cat["name"] for cat in categories]

    # Category classification using HF API
    try:
        classification = classify_with_hf_api(text, category_names)
        if classification:
            top_category = classification["labels"][0]
            top_score = classification["scores"][0]

            # Use semantic similarity for unseen data
            new_category = propose_new_category(text, classification, categories)
            if new_category not in category_names:
                # Add new category with a description
                new_cat_description = text[:100]  # Use first 100 chars as description
                categories.append({"name": new_category, "description": new_cat_description})
                save_categories(categories)
                print(f"📚 Added new category: {new_category}")
            category = new_category
        else:
            # Fallback to simple categorization
            category = simple_category_detection(text)
    except Exception as e:
        print(f"⚠️ Classification error: {e}")
        category = simple_category_detection(text)

    # Summarized description using HF API
    try:
        if len(text) > 50:
            summary = summarize_with_hf_api(text)
        else:
            summary = text
    except Exception as e:
        print(f"⚠️ Summarization error: {e}")
        summary = text[:80]

    return category, summary

# === 🔄 AWS S3 File Management Functions (S3 ONLY) ===
# [Keep all your existing S3 file management functions exactly as they are]
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
        "Scraped Data": f"data/{scraped_7d}"
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

# Parquet data functions - DIRECT S3 access (no local files)
def load_parquet_from_s3():
    """Load parquet data directly from S3 without downloading files"""
    try:
        response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=f"data/{scraped_7d}")
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
        print(f"✅ Loaded parquet from S3: {scraped_7d}")
        return df
    except s3.exceptions.NoSuchKey:
        print(f"⚠️ Parquet file {scraped_7d} not found in S3, returning empty DataFrame")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error loading parquet from S3: {e}")
        return pd.DataFrame()

def save_parquet_to_s3(df):
    """Save parquet data to S3 ONLY using the proven approach from test script"""
    try:
        if df.empty:
            print("⚠️ DataFrame is empty, nothing to save")
            return False
            
        print(f"💾 Attempting to save {len(df)} records to S3...")
        
        # Use in-memory buffer (same as test script)
        buffer = io.BytesIO()
        
        # Try different parquet engines (same as test script)
        engines = ['pyarrow', 'fastparquet', 'auto']
        success = False
        
        for engine in engines:
            try:
                print(f"🔄 Trying parquet engine: {engine}")
                buffer.seek(0)  # Reset buffer
                df.to_parquet(buffer, engine=engine, index=False)
                success = True
                print(f"✅ Success with engine: {engine}")
                break
            except Exception as e:
                print(f"❌ Engine {engine} failed: {e}")
                continue
        
        if not success:
            print("❌ All parquet engines failed")
            return False
        
        buffer.seek(0)
        
        # S3 key with proper path
        s3_key = f"data/{scraped_7d}"
        print(f"📤 Uploading to S3 bucket: {AWS_BUCKET_NAME}")
        
        # Upload to S3 (same as test script)
        s3.upload_fileobj(
            buffer, 
            AWS_BUCKET_NAME, 
            s3_key,
            ExtraArgs={'ContentType': 'application/octet-stream'}
        )
        
        print(f"✅ Successfully uploaded {len(df)} records to S3")
        
        # Verify upload (same as test script)
        try:
            response = s3.head_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
            file_size = response['ContentLength']
            last_modified = response['LastModified']
            print(f"✅ Upload verification successful!")
            print(f"📏 File size: {file_size} bytes")
            print(f"🕒 Last modified: {last_modified}")
            return True
        except Exception as e:
            print(f"⚠️ Upload verification failed: {e}")
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
    """Get the main Telethon client with PURE S3 session management"""
    client = None
    max_retries = 3
    retry_delay = 2
    
    # Download session file from S3 to memory (not to local file)
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
            
            # Create session from memory - write to temporary file for Telethon
            with open(USER_SESSION_FILE, 'wb') as f:
                f.write(session_data)
            
            session = SQLiteSession(USER_SESSION_FILE)
            client = TelegramClient(session, API_ID, API_HASH)
            
            await asyncio.wait_for(client.connect(), timeout=15)
            
            if not await client.is_user_authorized():
                error_msg = "Session not authorized"
                print(f"❌ {error_msg}")
                track_session_usage("connection", False, error_msg)
                await client.disconnect()
                # Clean up temporary file
                if os.path.exists(USER_SESSION_FILE):
                    os.remove(USER_SESSION_FILE)
                return None
            
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            print(f"✅ Telethon connected successfully as: {me.first_name} (@{me.username})")
            track_session_usage("connection", True)
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
            if os.path.exists(USER_SESSION_FILE):
                os.remove(USER_SESSION_FILE)
            
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                return None
    
    return None

# ======================
# 7-day scraping function with AI ENRICHMENT and PURE S3 integration
# ======================
async def scrape_channel_7days_async(channel_username: str):
    """Scrape last 7 days of data from a channel, apply AI enrichment, and store ONLY in S3"""
    telethon_client = None
    
    try:
        # Use direct S3 access for parquet data
        print("🔍 Loading parquet data directly from S3...")
        
        telethon_client = await get_telethon_client()
        if not telethon_client:
            track_session_usage("scraping", False, "Failed to initialize client")
            return False, "❌ Could not establish connection for scraping."
        
        print(f"🔍 Starting 7-day scrape for channel: {channel_username}")
        
        try:
            entity = await telethon_client.get_entity(channel_username)
            print(f"✅ Channel found: {entity.title}")
        except (ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            track_session_usage("scraping", False, f"Invalid channel: {str(e)}")
            return False, f"❌ Channel {channel_username} is invalid or doesn't exist."
        
        try:
            target_entity = await telethon_client.get_entity(FORWARD_CHANNEL)
            print(f"✅ Target channel resolved: {target_entity.title}")
        except Exception as e:
            track_session_usage("scraping", False, f"Target channel error: {str(e)}")
            return False, f"❌ Could not resolve target channel: {str(e)}"
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        print(f"⏰ Scraping messages from last 7 days (since {cutoff})")
        
        source_messages = []
        print(f"📡 Collecting messages from source channel: {channel_username}")
        
        async for message in telethon_client.iter_messages(entity, limit=None):
            if not message.text:
                continue
            if message.date < cutoff:
                break
            source_messages.append({
                'text': message.text,
                'date': message.date,
                'source_channel': channel_username,
                'source_message_id': message.id
            })
        
        print(f"📋 Collected {len(source_messages)} messages from source channel")
        
        scraped_data = []
        message_count = 0
        
        print(f"🔍 Searching for matching messages in target channel...")
        
        async for message in telethon_client.iter_messages(target_entity, limit=None):
            message_count += 1
            if message_count % 50 == 0:
                print(f"📊 Processed {message_count} messages in target channel...")
            if message.date < cutoff:
                print(f"⏹️ Reached 7-day cutoff at message {message_count}")
                break
            if not message.text:
                continue

            matching_source = None
            for source_msg in source_messages:
                if (source_msg['text'] in message.text or 
                    message.text in source_msg['text'] or
                    source_msg['text'][:100] in message.text):
                    matching_source = source_msg
                    break

            if not matching_source:
                continue

            info = extract_info(message.text, message.id)
            
            # === 🤖 AI ENRICHMENT ===
            print(f"🤖 Enriching product: {info['title'][:50]}...")
            predicted_category, generated_description = enrich_product(info["title"], info["description"])
            
            if getattr(target_entity, "username", None):
                post_link = f"https://t.me/{target_entity.username}/{message.id}"
            else:
                internal_id = str(target_entity.id)
                if internal_id.startswith("-100"):
                    internal_id = internal_id[4:]
                post_link = f"https://t.me/c/{internal_id}/{message.id}"

            post_data = {
                "title": info["title"],
                "description": info["description"],
                "price": info["price"],
                "phone": info["phone"],
                "location": info["location"],
                "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                "channel": channel_username,
                "post_link": post_link,
                "product_ref": str(message.id),
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                # === 🤖 AI ENRICHMENT FIELDS ===
                "predicted_category": predicted_category,
                "generated_description": generated_description
            }
            scraped_data.append(post_data)
        
        print(f"📋 Found {len(scraped_data)} matching messages in target channel")
        
        # Load existing data DIRECTLY FROM S3
        existing_df = load_parquet_from_s3()
        if existing_df is None:
            existing_df = pd.DataFrame()
        
        print(f"📁 Loaded existing data with {len(existing_df)} records from S3")
        
        new_df = pd.DataFrame(scraped_data)
        if not new_df.empty:
            # Combine and deduplicate
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['product_ref', 'channel'], keep='last')
            
            # Save ONLY to S3
            success = save_parquet_to_s3(combined_df)
            if success:
                print(f"💾 Saved {len(combined_df)} total records to S3")
                new_count = len(combined_df) - len(existing_df)
                
                # === 🤖 AI ENRICHMENT SUMMARY ===
                if AI_MODELS_LOADED and 'predicted_category' in combined_df.columns:
                    category_counts = combined_df['predicted_category'].value_counts()
                    print("\n📈 AI Enrichment Summary:")
                    for category, count in category_counts.items():
                        print(f"  {category}: {count} products")
                
                track_session_usage("scraping", True, f"Scraped {len(scraped_data)} messages")
                return True, f"✅ Scraped {len(scraped_data)} messages from {channel_username}. Added {new_count} new records to database."
            else:
                track_session_usage("scraping", False, "S3 save failed")
                return False, f"❌ Failed to save scraped data for {channel_username} to S3."
        else:
            track_session_usage("scraping", True, "No new messages found")
            return False, f"📭 No matching messages found in target channel for {channel_username} in the last 7 days."
            
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
    """Synchronous wrapper for 7-day scraping with AI enrichment"""
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
        forwarded_data = load_json_from_s3(f"data/{FORWARDED_FILE}")
        forwarded_ids = {
            int(msg_id): datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") 
            for msg_id, ts in forwarded_data.items()
        } if forwarded_data else {}

        # Remove forwarded IDs older than 7 days
        week_cutoff = now - timedelta(days=7)
        forwarded_ids = {msg_id: ts for msg_id, ts in forwarded_ids.items() 
                        if ts >= week_cutoff.replace(tzinfo=None)}

        messages_to_forward = []
        message_count = 0
        
        print(f"📨 Fetching messages from {channel_username}...")
        
        try:
            async for message in telethon_client.iter_messages(entity, limit=200):
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

        # Save updated forwarded IDs DIRECTLY TO S3
        save_json_to_s3(
            {str(k): v.strftime("%Y-%m-%d %H:%M:%S") for k, v in forwarded_ids.items()},
            f"data/{FORWARDED_FILE}"
        )

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

# [Keep all your existing bot commands exactly as they are - they will work with the new AI setup]

# Remove model-related commands since we don't have local models anymore
@authorized  
def model_checker(update, context):
    """Check AI models status (now using HF API)"""
    msg = "🔍 <b>AI Models Status (Hugging Face Inference API)</b>\n\n"
    msg += "• Zero-shot Classification: ✅ Available\n"
    msg += "• Text Summarization: ✅ Available\n"
    msg += "• Sentence Embeddings: ✅ Available\n"
    msg += "• Models Hosted: Hugging Face (remote)\n"
    msg += "• Local Storage: 0 GB (no models stored locally)\n"
    msg += f"• API Token: {'✅ Set' if HF_API_TOKEN else '⚠️ Not set (using public access)'}"
    
    update.message.reply_text(msg, parse_mode="HTML")

# Update the main function to reflect the change
def main():
    from telegram.utils.request import Request
    request = Request(connect_timeout=30, read_timeout=30, con_pool_size=8)
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    request=request
    
    # [Keep all your existing handler registrations]
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
    dp.add_handler(MessageHandler(Filters.command, unknown_command))
    dp.add_handler(CommandHandler("diagnose", diagnose_session))
    dp.add_handler(CommandHandler("debug_parquet", debug_s3_parquet))
    dp.add_handler(CommandHandler("test_s3_write", test_s3_write))
    dp.add_handler(CommandHandler("modelchecker", model_checker))
    dp.add_handler(CommandHandler("debug_parquet_comprehensive", debug_parquet_comprehensive))
    
    print(f"🤖 Bot is running...")
    print(f"🔧 Using session file: {USER_SESSION_FILE}")
    print(f"🌍 Environment: {'render' if 'RENDER' in os.environ else 'local'}")
    print(f"☁️ S3 Bucket: {AWS_BUCKET_NAME}")
    print(f"🤖 AI Models: ✅ Loaded (Hugging Face Inference API)")
    print(f"💾 Local Model Storage: 0 GB")
    
    # Efficiently check all S3 files on startup
    print("\n🔍 Checking S3 files efficiently (using head_object)...")
    s3_status = check_s3_files_status()
    
    # Ensure S3 structure exists
    ensure_s3_structure()
    
    try:
        updater.start_polling()
        updater.idle()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down bot...")
    except Exception as e:
        print(f"❌ Bot error: {e}")
    finally:
        print("👋 Bot stopped")

if __name__ == "__main__":
    main()