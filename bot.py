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
USER_SESSION_FILE = "user_session.session"  # Main user session file
scraped_7d = "scraped_7d.parquet" 
# === ⚡ MongoDB Setup ===
client = MongoClient(MONGO_URI)
db = client["yetal"]
channels_collection = db["yetalcollection"]
auth_collection = db["authorized_users"]  # store authorized user IDs

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

def cleanup_telethon_sessions(channel_username=None):
    """Clean up Telethon session files for specific channels (not the main user session)"""
    try:
        if channel_username:
            # Clean up specific channel session files
            session_pattern = f"session_{channel_username}.*"
            files = glob.glob(session_pattern)
            for file in files:
                os.remove(file)
                print(f"🧹 Deleted session file: {file}")
        else:
            # Clean up all temporary session files except the main user session
            session_files = glob.glob("session_*.*")
            for file in session_files:
                # Don't delete the main user session file
                if file == USER_SESSION_FILE or file.startswith(USER_SESSION_FILE.replace('.session', '')):
                    continue
                os.remove(file)
                print(f"🧹 Deleted session file: {file}")
    except Exception as e:
        print(f"❌ Error cleaning up session files: {e}")

# ======================
# Get or create main Telethon client with user session
# ======================
async def get_telethon_client():
    """Get the main Telethon client using the user session file"""
    try:
        client = TelegramClient(USER_SESSION_FILE, API_ID, API_HASH)
        
        # Connect the client first
        await client.connect()
        
        # Check if authorized
        if not await client.is_user_authorized():
            print("🔐 User not authorized. Please check the session file or re-authenticate.")
            await client.disconnect()
            return None
            
        return client
    except Exception as e:
        print(f"❌ Error creating Telethon client: {e}")
        # Ensure client is disconnected if there's an error
        try:
            await client.disconnect()
        except:
            pass
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
            return False, "❌ Failed to initialize Telethon client. Please check session."
        
        print(f"🔍 Checking if channel {channel_username} exists...")
        
        # First, verify the channel exists and we can access it
        try:
            entity = await telethon_client.get_entity(channel_username)
            print(f"✅ Channel found: {entity.title}")
        except (ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            print(f"❌ Channel error: {e}")
            await telethon_client.disconnect()
            return False, f"❌ Channel {channel_username} is invalid or doesn't exist."
        except Exception as e:
            print(f"❌ Unexpected error getting entity: {e}")
            await telethon_client.disconnect()
            return False, f"❌ Error accessing channel: {str(e)}"

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        print(f"⏰ Cutoff time: {cutoff}")

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
        async for message in telethon_client.iter_messages(channel_username, limit=200):
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

        print(f"📋 Found {len(messages_to_forward)} new messages to forward")

        if not messages_to_forward:
            await telethon_client.disconnect()
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

        # Disconnect the client
        await telethon_client.disconnect()

        if total_forwarded > 0:
            return True, f"✅ Successfully forwarded {total_forwarded} new posts from {channel_username}."
        else:
            return False, f"📭 No new posts to forward from {channel_username}."

    except Exception as e:
        print(f"❌ Critical error in forward_last_7d: {e}")
        # Ensure client is disconnected even if there's an error
        try:
            if telethon_client:
                await telethon_client.disconnect()
        except:
            pass
        return False, f"❌ Critical error: {str(e)}"

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
        print(f"❌ Error in forward_last_7d_sync: {e}")
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
                    result = f"✅ Session setup successful!\nLogged in as: {me.first_name} (@{me.username})"
                    
                    return result
                except Exception as e:
                    return f"❌ Session setup failed: {e}"
                finally:
                    if client:
                        await client.disconnect()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(setup_async())
            loop.close()
            
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=result
            )
            
        except Exception as e:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Session setup error: {e}"
            )
    
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
                    client = TelegramClient(USER_SESSION_FILE, API_ID, API_HASH)
                    await client.connect()
                    
                    if not await client.is_user_authorized():
                        return "❌ Session not authorized. Please run /setup_session first."
                    
                    me = await client.get_me()
                    result = f"✅ Session is valid!\nLogged in as: {me.first_name} (@{me.username})"
                    
                    return result
                except Exception as e:
                    return f"❌ Session check failed: {e}"
                finally:
                    # Always disconnect
                    if client:
                        await client.disconnect()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(check_async())
            loop.close()
            
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=result
            )
            
        except Exception as e:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Session check error: {e}"
            )
    
    threading.Thread(target=run_check, daemon=True).start()

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
        # Use the main user session client
        telethon_client = await get_telethon_client()
        if not telethon_client:
            return False, "❌ Failed to initialize Telethon client for scraping."
        
        print(f"🔍 Starting 7-day scrape for channel: {channel_username}")
        
        # Verify the channel exists
        try:
            entity = await telethon_client.get_entity(channel_username)
            print(f"✅ Channel found: {entity.title}")
        except (ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError) as e:
            await telethon_client.disconnect()
            return False, f"❌ Channel {channel_username} is invalid or doesn't exist."
        
        # ✅ Resolve target channel (forwarded copies live here)
        try:
            target_entity = await telethon_client.get_entity(FORWARD_CHANNEL)
            print(f"✅ Target channel resolved: {target_entity.title}")
        except Exception as e:
            print(f"❌ Could not resolve target channel {FORWARD_CHANNEL}: {e}")
            await telethon_client.disconnect()
            return False, f"❌ Could not resolve target channel: {str(e)}"
        
        # Calculate 7-day cutoff
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=7)
        print(f"⏰ Scraping messages from last 7 days (since {cutoff})")
        
        # ✅ FIRST: Collect all message texts from source channel to search for in target channel
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
        
        # ✅ SECOND: Iterate through target channel and match messages
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

            # ✅ Find matching message from source channel
            matching_source = None
            for source_msg in source_messages:
                # Simple text matching - same logic as working version
                if (source_msg['text'] in message.text or 
                    message.text in source_msg['text'] or
                    source_msg['text'][:100] in message.text):  # Match first 100 chars
                    matching_source = source_msg
                    break

            if not matching_source:
                continue

            info = extract_info(message.text, message.id)
            
            # ✅ Build permalink from TARGET channel (where the message actually exists)
            if getattr(target_entity, "username", None):
                post_link = f"https://t.me/{target_entity.username}/{message.id}"
            else:
                internal_id = str(target_entity.id)
                if internal_id.startswith("-100"):
                    internal_id = internal_id[4:]
                post_link = f"https://t.me/c/{internal_id}/{message.id}"

            # Create post data
            post_data = {
                "title": info["title"],
                "description": info["description"],
                "price": info["price"],
                "phone": info["phone"],
                "location": info["location"],
                "date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
                "channel": channel_username,
                "post_link": post_link,
                "product_ref": str(message.id),  # Use the target channel message ID
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            scraped_data.append(post_data)
        
        print(f"📋 Found {len(scraped_data)} matching messages in target channel")
        
        # Load existing data and remove duplicates
        existing_df = pd.DataFrame()
        if os.path.exists(scraped_7d):
            existing_df = pd.read_parquet(scraped_7d, engine='pyarrow')
            print(f"📁 Loaded existing data with {len(existing_df)} records")
        
        # Create new dataframe and remove duplicates
        new_df = pd.DataFrame(scraped_data)
        if not new_df.empty:
            # Combine existing and new data, remove duplicates based on product_ref AND channel
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Remove duplicates based on both product_ref and channel to avoid same message ID from different channels
            combined_df = combined_df.drop_duplicates(subset=['product_ref', 'channel'], keep='last')
            
            # Save updated data
            combined_df.to_parquet(scraped_7d, engine='pyarrow', index=False)
            print(f"💾 Saved {len(combined_df)} total records to {scraped_7d}")
            
            new_count = len(combined_df) - len(existing_df)
            await telethon_client.disconnect()
            return True, f"✅ Scraped {len(scraped_data)} messages from {channel_username}. Added {new_count} new records to database."
        else:
            await telethon_client.disconnect()
            return False, f"📭 No matching messages found in target channel for {channel_username} in the last 7 days."
            
    except Exception as e:
        print(f"❌ Error in 7-day scraping: {e}")
        if telethon_client:
            await telethon_client.disconnect()
        return False, f"❌ Scraping error: {str(e)}"
        
def scrape_channel_7days_sync(channel_username: str):
    """Synchronous wrapper for 7-day scraping"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(scrape_channel_7days_async(channel_username))
        loop.close()
        return result
    except Exception as e:
        return False, f"❌ Scraping error: {str(e)}"

def scrape_channel_7days_sync(channel_username: str):
    """Synchronous wrapper for 7-day scraping"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(scrape_channel_7days_async(channel_username))
        loop.close()
        return result
    except Exception as e:
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
        update.message.reply_text(
            "❌ Please provide a valid channel username starting with @"
        )
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

        # Start 7-day scraping in background
        update.message.reply_text(f"⏳ Starting 7-day data scraping from {username}...")
        
        def run_scraping():
            try:
                success, result_msg = scrape_channel_7days_sync(username)
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=result_msg,
                    parse_mode="HTML"
                )
            except Exception as e:
                error_msg = f"❌ Error during scraping: {str(e)}"
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=error_msg,
                    parse_mode="HTML"
                )

        # Start scraping in background thread
        threading.Thread(target=run_scraping, daemon=True).start()

        # Also start forwarding in background (your existing forwarding code)
        update.message.reply_text(f"⏳ Forwarding last 7d posts from {username}...")
        
        def run_forwarding():
            try:
                success, result_msg = forward_last_7d_sync(username)
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=result_msg,
                    parse_mode="HTML"
                )
            except Exception as e:
                error_msg = f"❌ Error during forwarding: {str(e)}"
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=error_msg,
                    parse_mode="HTML"
                )

        threading.Thread(target=run_forwarding, daemon=True).start()

    except BadRequest as e:
        update.message.reply_text(
            f"❌ Could not add channel:\n<code>{str(e)}</code>", parse_mode="HTML"
        )
    except Exception as e:
        update.message.reply_text(
            f"❌ Unexpected error:\n<code>{str(e)}</code>", parse_mode="HTML"
        )
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
        update.message.reply_text(
            "❌ Please provide a valid channel username starting with @"
        )
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
        update.message.reply_text(
            f"❌ Channel {username} is not in the database.", parse_mode="HTML"
        )

@authorized
def delete_channel(update, context):
    if len(context.args) == 0:
        update.message.reply_text("⚡ Usage: /deletechannel @ChannelUsername")
        return

    username = context.args[0].strip()
    if not username.startswith("@"):
        update.message.reply_text(
            "❌ Please provide a valid channel username starting with @"
        )
        return

    result = channels_collection.delete_one({"username": username})
    if result.deleted_count > 0:
        update.message.reply_text(
            f"✅ Channel {username} has been deleted from the database."
        )
    else:
        update.message.reply_text(
            f"⚠️ Channel {username} was not found in the database."
        )

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
        "/test - Test connection\n"
        
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
                    client = TelegramClient(USER_SESSION_FILE, API_ID, API_HASH)
                    await client.connect()
                    
                    if not await client.is_user_authorized():
                        return "❌ Session not authorized. Please run /setup_session first."
                    
                    me = await client.get_me()
                    result = f"✅ Telethon connected as: {me.first_name} (@{me.username})"
                    
                    try:
                        target = await client.get_entity(FORWARD_CHANNEL)
                        result += f"\n✅ Target channel accessible: {target.title}"
                    except Exception as e:
                        result += f"\n❌ Cannot access target channel {FORWARD_CHANNEL}: {e}"
                    
                    return result
                except Exception as e:
                    return f"❌ Telethon connection error: {e}"
                finally:
                    if client:
                        await client.disconnect()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_async())
            loop.close()
            
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=result
            )
            
        except Exception as e:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Test failed: {e}"
            )
    
    # Run test in background thread
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
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("code", code))
    dp.add_handler(CommandHandler("addchannel", add_channel))
    dp.add_handler(CommandHandler("listchannels", list_channels))
    dp.add_handler(CommandHandler("checkchannel", check_channel))
    dp.add_handler(CommandHandler("deletechannel", delete_channel))
    dp.add_handler(CommandHandler("setup_session", setup_session))
    dp.add_handler(CommandHandler("check_session", check_session))
    dp.add_handler(CommandHandler("test", test_connection))
    dp.add_handler(CommandHandler("check_data", check_scraped_data))
   
    dp.add_handler(MessageHandler(Filters.command, unknown_command))

    print("🤖 Bot is running...")
    
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