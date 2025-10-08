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

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is alive!"

# Run Flask in a separate thread
def run_flask():
    app.run(host="0.0.0.0", port=5000)

threading.Thread(target=run_flask, daemon=True).start()

# === 🔐 Load environment variables ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
API_ID = 24916488
API_HASH = "3b7788498c56da1a02e904ff8e92d494"
FORWARD_CHANNEL = os.getenv("FORWARD_CHANNEL")  # target channel username
ADMIN_CODE = os.getenv("ADMIN_CODE")  # secret code for access

# === 📁 Files ===
FORWARDED_FILE = "forwarded_messages.json"
scraped_7d = "scraped_7d.parquet" 

# === 🔧 Environment Detection ===
def get_session_filename():
    """Get unique session filename for each environment"""
    # Detect if running on Render
    if 'RENDER' in os.environ:
        return "session_render.session"
    
    # Detect if running locally by computer name or other indicators
    computer_name = platform.node().lower()
    username = os.getenv('USER', '').lower()
    
    # Common local computer identifiers
    local_indicators = ['desktop', 'laptop', 'pc', 'home', 'workstation', 'macbook']
    
    if any(indicator in computer_name for indicator in local_indicators):
        return "session_local.session"
    elif username and username not in ['render', 'root', 'admin']:  # Common local usernames
        return "session_local.session"
    elif os.name == 'nt':  # Windows
        return "session_local.session"
    else:
        return "session_main.session"

USER_SESSION_FILE = get_session_filename()
print(f"🔧 Environment detected: Using session file - {USER_SESSION_FILE}")

# === ⚡ MongoDB Setup ===
client = MongoClient(MONGO_URI)
db = client["yetal"]
channels_collection = db["yetalcollection"]
auth_collection = db["authorized_users"]  # store authorized user IDs
session_usage_collection = db["session_usage"]  # track session usage

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
        # Last 24 hours stats
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
        
        # Recent errors
        recent_errors = list(session_usage_collection.find({
            "timestamp": {"$gte": day_ago},
            "success": False
        }).sort("timestamp", -1).limit(5))
        
        # Environment usage
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
# Session Management
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
    """Get the main Telethon client with robust error handling and retry logic"""
    client = None
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"🔧 Attempt {attempt + 1}/{max_retries} to connect Telethon client...")
            
            # Create session with explicit handling
            session = SQLiteSession(USER_SESSION_FILE)
            client = TelegramClient(session, API_ID, API_HASH)
            
            # Connect with timeout
            await asyncio.wait_for(client.connect(), timeout=15)
            
            # Check if authorized
            if not await client.is_user_authorized():
                error_msg = "Session not authorized"
                print(f"❌ {error_msg}")
                track_session_usage("connection", False, error_msg)
                await client.disconnect()
                return None
            
            # Verify connection by getting user info
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            print(f"✅ Telethon connected successfully as: {me.first_name} (@{me.username})")
            track_session_usage("connection", True)
            return client
            
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
            error_msg = f"Database locked/error (attempt {attempt + 1})"
            print(f"🔄 {error_msg}: {e}")
            track_session_usage("connection", False, error_msg)
            
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("❌ Max retries reached for database connection")
                return None
                
        except asyncio.TimeoutError:
            error_msg = f"Connection timeout (attempt {attempt + 1})"
            print(f"⏰ {error_msg}")
            track_session_usage("connection", False, error_msg)
            
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                return None
                
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            print(f"❌ {error_msg}")
            track_session_usage("connection", False, error_msg)
            
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                return None
    
    return None

# ======================
# Forward last 7d posts from a given channel with duplicate prevention
# ======================
async def forward_last_7d_async(channel_username: str):
    """Async function to forward messages using the main Telethon client"""
    telethon_client = None
    
    try:
        # Use the main user session client
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

        # Load previously forwarded messages with timestamps
        if os.path.exists(FORWARDED_FILE):
            with open(FORWARDED_FILE, "r") as f:
                forwarded_data = json.load(f)
                forwarded_ids = {
                    int(msg_id): datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") 
                    for msg_id, ts in forwarded_data.items()
                }
        else:
            forwarded_ids = {}

        # Remove forwarded IDs older than 7 days
        week_cutoff = now - timedelta(days=7)
        forwarded_ids = {msg_id: ts for msg_id, ts in forwarded_ids.items() 
                        if datetime.strptime(forwarded_data[str(msg_id)], "%Y-%m-%d %H:%M:%S") >= week_cutoff.replace(tzinfo=None)}

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
                # Add timeout to avoid hanging forever
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
                await asyncio.sleep(1)  # Small delay between batches
                
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

        # Save updated forwarded IDs
        with open(FORWARDED_FILE, "w") as f:
            json.dump({str(k): v.strftime("%Y-%m-%d %H:%M:%S") for k, v in forwarded_ids.items()}, f)

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
        # Ensure client is disconnected even if there's an error
        if telethon_client:
            try:
                await telethon_client.disconnect()
            except:
                pass

def forward_last_7d_sync(channel_username: str):
    """Synchronous wrapper for the async forwarding function"""
    try:
        # Create a new event loop for this operation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(forward_last_7d_async(channel_username))
        loop.close()
        return result
    except Exception as e:
        track_session_usage("forwarding", False, f"Sync error: {str(e)}")
        return False, f"❌ Error: {str(e)}"

# ======================
# Session management commands
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
                    
                    # This will prompt for phone number and code on first run
                    me = await client.get_me()
                    result = f"✅ Session setup successful!\nLogged in as: {me.first_name} (@{me.username})\n\nSession file: {USER_SESSION_FILE}"
                    track_session_usage("setup", True)
                    return result
                except Exception as e:
                    error_msg = f"Session setup failed: {e}"
                    track_session_usage("setup", False, error_msg)
                    return f"❌ {error_msg}"
                finally:
                    if client:
                        await client.disconnect()
            
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
        msg += f"💻 <b>Computer:</b> {platform.node()}\n\n"
        
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
# /start command
# ======================
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
            "/cleanup - Cleanup sessions\n"
            "/clearhistory - Clear forwarded history"
        )
    else:
        update.message.reply_text(
            "⚡ Welcome! Please enter your access code using /code YOUR_CODE"
        )

# ======================
# /code command
# ======================
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

# === 📊 7-day scraping function ===
async def scrape_channel_7days_async(channel_username: str):
    """Scrape last 7 days of data from a channel and append to parquet file"""
    telethon_client = None
    
    try:
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
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            scraped_data.append(post_data)
        
        print(f"📋 Found {len(scraped_data)} matching messages in target channel")
        
        existing_df = pd.DataFrame()
        if os.path.exists(scraped_7d):
            existing_df = pd.read_parquet(scraped_7d, engine='pyarrow')
            print(f"📁 Loaded existing data with {len(existing_df)} records")
        
        new_df = pd.DataFrame(scraped_data)
        if not new_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['product_ref', 'channel'], keep='last')
            combined_df.to_parquet(scraped_7d, engine='pyarrow', index=False)
            print(f"💾 Saved {len(combined_df)} total records to {scraped_7d}")
            
            new_count = len(combined_df) - len(existing_df)
            track_session_usage("scraping", True, f"Scraped {len(scraped_data)} messages")
            return True, f"✅ Scraped {len(scraped_data)} messages from {channel_username}. Added {new_count} new records to database."
        else:
            track_session_usage("scraping", True, "No new messages found")
            return False, f"📭 No matching messages found in target channel for {channel_username} in the last 7 days."
            
    except Exception as e:
        error_msg = f"Scraping error: {str(e)}"
        track_session_usage("scraping", False, error_msg)
        return False, f"❌ Scraping error: {str(e)}"
    finally:
        if telethon_client:
            try:
                await telethon_client.disconnect()
            except:
                pass

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
# Bot commands
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

        # Run operations sequentially
        def run_operations():
            try:
                # First scraping
                update.message.reply_text(f"⏳ Starting 7-day data scraping from {username}...")
                success, result_msg = scrape_channel_7days_sync(username)
                context.bot.send_message(update.effective_chat.id, text=result_msg, parse_mode="HTML")
                
                # Then forwarding with delay
                import time
                time.sleep(2)
                update.message.reply_text(f"⏳ Forwarding last 7d posts from {username}...")
                success, result_msg = forward_last_7d_sync(username)
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
def check_scraped_data(update, context):
    """Check the current scraped data statistics"""
    try:
        if os.path.exists(scraped_7d):
            df = pd.read_parquet(scraped_7d, engine='pyarrow')
            channel_counts = df['channel'].value_counts()
            
            msg = f"📊 <b>Scraped Data Summary:</b>\n"
            msg += f"Total records: {len(df)}\n\n"
            msg += "<b>Records per channel:</b>\n"
            
            for channel, count in channel_counts.items():
                msg += f"• {channel}: {count} records\n"
                
            update.message.reply_text(msg, parse_mode="HTML")
        else:
            update.message.reply_text("📭 No scraped data found yet.")
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
def unknown_command(update, context):
    update.message.reply_text(
        "❌ Unknown command.\n\n"
        "👉 Available commands:\n"
        "/addchannel @ChannelUsername\n"
        "/listchannels\n"
        "/checkchannel @ChannelUsername\n"
        "/deletechannel @ChannelUsername\n"
        "/setup_session - Set up Telegram session\n"
        "/check_session - Check session status\n"
        "/checksessionusage - Session usage stats\n"
        "/test - Test connection\n"
        "/check_data - Check scraped data\n"
        "/cleanup - Cleanup sessions\n"
        "/clearhistory - Clear forwarded history"
    )

# ======================
# Test command to check telethon connection
# ======================
@authorized
def test_connection(update, context):
    """Test if Telethon client is working"""
    def run_test():
        try:
            async def test_async():
                client = None
                try:
                    client = await get_telethon_client()
                    if not client:
                        return "❌ Could not establish connection. Check /checksessionusage for details."
                    
                    me = await client.get_me()
                    result = f"✅ Telethon connected as: {me.first_name} (@{me.username})\n\n"
                    
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
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_async())
            loop.close()
            
            context.bot.send_message(update.effective_chat.id, text=result)
            
        except Exception as e:
            context.bot.send_message(update.effective_chat.id, text=f"❌ Test failed: {e}")
    
    threading.Thread(target=run_test, daemon=True).start()

# ======================
# Cleanup command to remove all temporary session files
# ======================
@authorized
def cleanup_sessions(update, context):
    """Clean up all temporary Telethon session files (except main user session)"""
    try:
        cleanup_telethon_sessions()
        update.message.reply_text("✅ All temporary session files have been cleaned up.")
    except Exception as e:
        update.message.reply_text(f"❌ Error cleaning up sessions: {e}")

# ======================
# Clear forwarded messages history
# ======================
@authorized
def clear_forwarded_history(update, context):
    """Clear the forwarded messages history file"""
    try:
        if os.path.exists(FORWARDED_FILE):
            os.remove(FORWARDED_FILE)
            update.message.reply_text("✅ Forwarded messages history cleared.")
        else:
            update.message.reply_text("ℹ️ No forwarded messages history found.")
    except Exception as e:
        update.message.reply_text(f"❌ Error clearing history: {e}")

# ======================
# Main
# ======================
def main():
    from telegram.utils.request import Request
    request = Request(connect_timeout=30, read_timeout=30, con_pool_size=8)
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    request=request

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
    dp.add_handler(CommandHandler("cleanup", cleanup_sessions))
    dp.add_handler(CommandHandler("clearhistory", clear_forwarded_history))
    dp.add_handler(MessageHandler(Filters.command, unknown_command))

    print(f"🤖 Bot is running...")
    print(f"🔧 Using session file: {USER_SESSION_FILE}")
    print(f"🌍 Environment: {'render' if 'RENDER' in os.environ else 'local'}")
    
    # Check if session file exists on startup
    if os.path.exists(USER_SESSION_FILE):
        print("✅ User session file found.")
    else:
        print("⚠️ User session file not found. Run /setup_session to create one.")
    
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