import asyncio
import logging
import json
from telethon import TelegramClient, events, errors
from telethon.tl.types import MessageMediaWebPage, MessageEntityTextUrl, MessageEntityUrl, MessageMediaPhoto, MessageMediaDocument
from collections import deque
from datetime import datetime
import emoji

API_ID = 23617139  # Your API ID
API_HASH = "5bfc582b080fa09a1a2eaa6ee60fd5d4"  # Your API hash
SESSION_FILE = "userbot_session"
client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# Configuration
MAPPINGS_FILE = "channel_mappings.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_QUEUE_SIZE = 100
MAX_MAPPING_HISTORY = 1000
MONITOR_CHAT_ID = None
NOTIFY_CHAT_ID = None  # Set this to the chat ID for notifications
INACTIVITY_THRESHOLD = 3600  # Notify if no activity for 1 hour (in seconds)
MAX_MESSAGE_LENGTH = 4096  # Telegram's max message length

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("forward_bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger("ForwardBot")

# Data structures
channel_mappings = {}
message_queue = deque(maxlen=MAX_QUEUE_SIZE)
is_connected = False
pair_stats = {}

def save_mappings():
    try:
        with open(MAPPINGS_FILE, "w") as f:
            json.dump(channel_mappings, f)
        logger.info("Channel mappings saved to file.")
    except Exception as e:
        logger.error(f"Error saving mappings: {e}")

def load_mappings():
    global channel_mappings
    try:
        with open(MAPPINGS_FILE, "r") as f:
            channel_mappings = json.load(f)
        logger.info(f"Loaded {sum(len(v) for v in channel_mappings.values())} mappings from file.")
        for user_id, pairs in channel_mappings.items():
            if user_id not in pair_stats:
                pair_stats[user_id] = {}
            for pair_name in pairs:
                pair_stats[user_id][pair_name] = {
                    'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
                }
    except FileNotFoundError:
        logger.info("No existing mappings file found. Starting fresh.")
    except Exception as e:
        logger.error(f"Error loading mappings: {e}")

async def process_message_queue():
    while message_queue and is_connected:
        message_data = message_queue.popleft()
        await forward_message_with_retry(*message_data)

def render_emoji(text):
    """Convert emoji text to actual emoji images."""
    return emoji.emojize(text, language='alias')

def filter_blacklisted_words(text, blacklist):
    if not text or not blacklist:
        return text
    for word in blacklist:
        text = text.replace(word, "***")
    return text

def check_blocked_sentences(text, blocked_sentences):
    if not text or not blocked_sentences:
        return False, None
    for sentence in blocked_sentences:
        if sentence.lower() in text.lower():
            return True, sentence
    return False, None

def filter_urls(text, block_urls, blacklist_urls=None):
    if not text or not block_urls:
        return text, True
    import re
    url_pattern = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^\s]*)?'
    urls = re.findall(url_pattern, text)
    if blacklist_urls:
        for url in urls:
            if any(blacklisted in url for blacklisted in blacklist_urls):
                text = text.replace(url, '[URL BLOCKED]')
        return text, True
    else:
        text = re.sub(url_pattern, '[URL REMOVED]', text)
        return text, False

def remove_header_footer(text, header_pattern, footer_pattern):
    if not text:
        return text
    if header_pattern and text.startswith(header_pattern):
        text = text[len(header_pattern):].strip()
    if footer_pattern and text.endswith(footer_pattern):
        text = text[:-len(footer_pattern)].strip()
    return text

def apply_custom_header_footer(text, custom_header, custom_footer):
    if not text:
        return text
    result = text
    if custom_header:
        result = f"{custom_header}\n{result}"
    if custom_footer:
        result = f"{result.rstrip()}\n{custom_footer}"
    return result.strip()

async def forward_message_with_retry(event, mapping, user_id, pair_name):
    for attempt in range(MAX_RETRIES):
        try:
            message_text = event.message.raw_text or ""
            original_entities = event.message.entities or []
            media = event.message.media
            is_webpage = isinstance(media, MessageMediaWebPage)

            # Check for link entities or webpage previews
            has_links = any(
                isinstance(e, (MessageEntityTextUrl, MessageEntityUrl))
                for e in original_entities
            )
            allow_preview = is_webpage or has_links  # Enable previews for URLs or webpages

            # Apply blocked sentences filter
            if mapping.get('blocked_sentences'):
                should_block, matching_sentence = check_blocked_sentences(message_text, mapping['blocked_sentences'])
                if should_block:
                    logger.info(f"Message blocked due to blocked sentence: '{matching_sentence}'")
                    pair_stats[user_id][pair_name]['blocked'] += 1
                    return True

            # Apply blacklist filter
            if mapping.get('blacklist') and message_text:
                message_text = filter_blacklisted_words(message_text, mapping['blacklist'])
                if message_text.strip() == "***":
                    logger.info("Message entirely blocked due to blacklist filter")
                    pair_stats[user_id][pair_name]['blocked'] += 1
                    return True

            # Apply URL filter
            if mapping.get('block_urls', False) or mapping.get('blacklist_urls'):
                message_text, allow_preview = filter_urls(
                    message_text,
                    mapping.get('block_urls', False),
                    mapping.get('blacklist_urls')
                )
                if message_text != event.message.raw_text and original_entities:
                    original_entities = None

            # Apply header/footer removal
            if (mapping.get('header_pattern') or mapping.get('footer_pattern')) and message_text:
                message_text = remove_header_footer(
                    message_text, mapping.get('header_pattern', ''), mapping.get('footer_pattern', '')
                )
                if message_text != event.message.raw_text:
                    original_entities = None

            # Apply mention removal
            if mapping.get('remove_mentions', False) and message_text:
                import re
                message_text = re.sub(r'@[a-zA-Z0-9_]+|\[([^\]]+)\]\(tg://user\?id=\d+\)', '', message_text)
                message_text = re.sub(r'\s+', ' ', message_text).strip()
                if message_text != event.message.raw_text:
                    original_entities = None

            # Skip if message is empty after filtering
            if not message_text.strip() and not media:
                logger.info("Message skipped: empty after filtering")
                pair_stats[user_id][pair_name]['blocked'] += 1
                return True

            # Apply custom header/footer
            message_text = apply_custom_header_footer(
                message_text, mapping.get('custom_header', ''), mapping.get('custom_footer', '')
            )
            if message_text != event.message.raw_text:
                original_entities = None

            # Render emojis
            message_text = render_emoji(message_text)

            # Handle replies
            reply_to = await handle_reply_mapping(event, mapping)

            # Prepare send parameters
            send_params = {
                'entity': int(mapping['destination']),
                'message': message_text,
                'link_preview': allow_preview,
                'reply_to': reply_to,
                'silent': event.message.silent,
                'formatting_entities': original_entities if original_entities else None,
                'parse_mode': None
            }

            # Only include 'file' for supported media types
            if media and isinstance(media, (MessageMediaPhoto, MessageMediaDocument)):
                send_params['file'] = media
            elif is_webpage:
                logger.info("Processing MessageMediaWebPage, using text and preview only")

            sent_message = await client.send_message(**send_params)
            await store_message_mapping(event, mapping, sent_message)
            pair_stats[user_id][pair_name]['forwarded'] += 1
            pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
            logger.info(f"Message forwarded from {mapping['source']} to {mapping['destination']} (ID: {sent_message.id})")
            return True

        except errors.FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"Flood wait error, sleeping for {wait_time} seconds...")
            await asyncio.sleep(wait_time)
        except (errors.RPCError, ConnectionError) as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"Failed to forward message after {MAX_RETRIES} attempts: {e}")
                if NOTIFY_CHAT_ID:
                    await client.send_message(
                        NOTIFY_CHAT_ID,
                        f"⚠️ Error: Failed to forward message for pair '{pair_name}' after {MAX_RETRIES} attempts. Error: {e}"
                    )
                return False
        except Exception as e:
            logger.error(f"Unexpected error forwarding message: {e}", exc_info=True)
            if NOTIFY_CHAT_ID:
                await client.send_message(
                    NOTIFY_CHAT_ID,
                    f"⚠️ Unexpected Error: Pair '{pair_name}' failed. Error: {e}"
                )
            return False

async def edit_forwarded_message(event, mapping, user_id, pair_name):
    try:
        mapping_key = f"{mapping['source']}:{event.message.id}"
        if not hasattr(client, 'forwarded_messages'):
            logger.warning("No forwarded_messages attribute found on client")
            return
        if mapping_key not in client.forwarded_messages:
            logger.warning(f"No mapping found for message: {mapping_key}")
            return

        forwarded_msg_id = client.forwarded_messages[mapping_key]
        forwarded_msg = await client.get_messages(int(mapping['destination']), ids=forwarded_msg_id)
        if not forwarded_msg:
            logger.warning(f"Forwarded message {forwarded_msg_id} not found in destination {mapping['destination']}")
            del client.forwarded_messages[mapping_key]
            return

        message_text = event.message.raw_text or ""
        original_entities = event.message.entities or []
        media = event.message.media
        is_webpage = isinstance(media, MessageMediaWebPage)
        has_links = any(
            isinstance(e, (MessageEntityTextUrl, MessageEntityUrl))
            for e in original_entities
        )
        allow_preview = is_webpage or has_links

        if mapping.get('blocked_sentences'):
            should_block, matching_sentence = check_blocked_sentences(message_text, mapping['blocked_sentences'])
            if should_block:
                await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
                logger.info(f"Forwarded message {forwarded_msg_id} deleted due to blocked sentence: '{matching_sentence}'")
                pair_stats[user_id][pair_name]['blocked'] += 1
                pair_stats[user_id][pair_name]['deleted'] += 1
                return

        if mapping.get('blacklist') and message_text:
            message_text = filter_blacklisted_words(message_text, mapping['blacklist'])
            if message_text.strip() == "***":
                await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
                logger.info(f"Forwarded message {forwarded_msg_id} deleted due to blacklist filter")
                pair_stats[user_id][pair_name]['blocked'] += 1
                pair_stats[user_id][pair_name]['deleted'] += 1
                return

        if mapping.get('block_urls', False) or mapping.get('blacklist_urls'):
            message_text, allow_preview = filter_urls(
                message_text, 
                mapping.get('block_urls', False), 
                mapping.get('blacklist_urls')
            )
            if message_text != event.message.raw_text:
                original_entities = None

        if (mapping.get('header_pattern') or mapping.get('footer_pattern')) and message_text:
            message_text = remove_header_footer(
                message_text, mapping.get('header_pattern', ''), mapping.get('footer_pattern', '')
            )
            if message_text != event.message.raw_text:
                original_entities = None

        if mapping.get('remove_mentions', False) and message_text:
            import re
            message_text = re.sub(r'@[a-zA-Z0-9_]+|\[([^\]]+)\]\(tg://user\?id=\d+\)', '', message_text)
            message_text = re.sub(r'\s+', ' ', message_text).strip()
            if message_text != event.message.raw_text:
                original_entities = None

        if not message_text.strip() and not media:
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            logger.info(f"Forwarded message {forwarded_msg_id} deleted: empty after filtering")
            pair_stats[user_id][pair_name]['blocked'] += 1
            pair_stats[user_id][pair_name]['deleted'] += 1
            return

        message_text = apply_custom_header_footer(
            message_text, mapping.get('custom_header', ''), mapping.get('custom_footer', '')
        )
        if message_text != event.message.raw_text:
            original_entities = None

        message_text = render_emoji(message_text)

        edit_params = {
            'entity': int(mapping['destination']),
            'message': forwarded_msg_id,
            'text': message_text,
            'link_preview': allow_preview,
            'formatting_entities': original_entities if original_entities else None,
            'parse_mode': None
        }
        if media and isinstance(media, (MessageMediaPhoto, MessageMediaDocument)):
            edit_params['file'] = media
        elif is_webpage:
            logger.info("Editing MessageMediaWebPage, using text and preview only")

        await client.edit_message(**edit_params)
        pair_stats[user_id][pair_name]['edited'] += 1
        pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
        logger.info(f"Forwarded message {forwarded_msg_id} edited in {mapping['destination']}")

    except errors.MessageAuthorRequiredError:
        logger.error(f"Cannot edit message {forwarded_msg_id}: Bot must be the original author")
    except errors.MessageIdInvalidError:
        logger.error(f"Cannot edit message {forwarded_msg_id}: Message ID is invalid or deleted")
        if mapping_key in client.forwarded_messages:
            del client.forwarded_messages[mapping_key]
    except errors.FloodWaitError as e:
        logger.warning(f"Flood wait error while editing, sleeping for {e.seconds} seconds...")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.error(f"Error editing forwarded message {forwarded_msg_id}: {e}")

async def delete_forwarded_message(event, mapping, user_id, pair_name):
    try:
        mapping_key = f"{mapping['source']}:{event.message.id}"
        if not hasattr(client, 'forwarded_messages'):
            logger.warning("No forwarded_messages attribute found on client")
            return
        if mapping_key not in client.forwarded_messages:
            logger.warning(f"No mapping found for deleted message: {mapping_key}")
            return

        forwarded_msg_id = client.forwarded_messages[mapping_key]
        await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
        pair_stats[user_id][pair_name]['deleted'] += 1
        pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
        logger.info(f"Forwarded message {forwarded_msg_id} deleted from {mapping['destination']}")
        del client.forwarded_messages[mapping_key]

    except errors.MessageIdInvalidError:
        logger.warning(f"Cannot delete message {forwarded_msg_id}: Already deleted or invalid")
        if mapping_key in client.forwarded_messages:
            del client.forwarded_messages[mapping_key]
    except Exception as e:
        logger.error(f"Error deleting forwarded message: {e}")

async def handle_reply_mapping(event, mapping):
    if not hasattr(event.message, 'reply_to') or not event.message.reply_to:
        return None
    try:
        source_reply_id = event.message.reply_to.reply_to_msg_id
        if not source_reply_id:
            return None
        mapping_key = f"{mapping['source']}:{source_reply_id}"
        if hasattr(client, 'forwarded_messages') and mapping_key in client.forwarded_messages:
            return client.forwarded_messages[mapping_key]
        replied_msg = await client.get_messages(int(mapping['source']), ids=source_reply_id)
        if replied_msg and replied_msg.text:
            dest_msgs = await client.get_messages(int(mapping['destination']), search=replied_msg.text[:20], limit=5)
            if dest_msgs:
                return dest_msgs[0].id
    except Exception as e:
        logger.error(f"Error handling reply mapping: {e}")
    return None

async def store_message_mapping(event, mapping, sent_message):
    try:
        if not hasattr(event.message, 'id'):
            return
        if not hasattr(client, 'forwarded_messages'):
            client.forwarded_messages = {}
        if len(client.forwarded_messages) >= MAX_MAPPING_HISTORY:
            oldest_key = next(iter(client.forwarded_messages))
            client.forwarded_messages.pop(oldest_key)
        source_msg_id = event.message.id
        mapping_key = f"{mapping['source']}:{source_msg_id}"
        client.forwarded_messages[mapping_key] = sent_message.id
    except Exception as e:
        logger.error(f"Error storing message mapping: {e}")

async def send_split_message(event, full_message):
    if len(full_message) <= MAX_MESSAGE_LENGTH:
        await event.reply(render_emoji(full_message))
        return

    parts = []
    current_part = ""
    for line in full_message.split('\n'):
        if len(current_part) + len(line) + 1 > MAX_MESSAGE_LENGTH:
            parts.append(current_part.strip())
            current_part = line + "\n"
        else:
            current_part += line + "\n"
    if current_part.strip():
        parts.append(current_part.strip())

    for i, part in enumerate(parts, 1):
        await event.reply(render_emoji(f"Part {i}/{len(parts)}\n{part}"))
        await asyncio.sleep(0.5)

@client.on(events.NewMessage(pattern='(?i)^/start$'))
async def start(event):
    await event.reply(render_emoji("✅ ForwardBot Running!\nUse `/commands` for options."))

@client.on(events.NewMessage(pattern='(?i)^/commands$'))
async def list_commands(event):
    commands = render_emoji("""
    📌 ForwardBot Commands

    Setup & Management
    - `/setpair <name> <source> <dest> [yes|no]` - Add a forwarding pair (yes/no for mentions)
    - `/listpairs` - Show all pairs
    - `/pausepair <name>` - Pause a pair
    - `/startpair <name>` - Resume a pair
    - `/clearpairs` - Remove all pairs
    - `/togglementions <name>` - Toggle mention removal
    - `/monitor` - View pair stats

    📋 Filters
    - `/addblacklist <name> <word1,word2,...>` - Blacklist words
    - `/clearblacklist <name>` - Clear blacklist
    - `/showblacklist <name>` - Show blacklist
    - `/toggleurlblock <name>` - Toggle URL blocking
    - `/addurlblacklist <name> <url1,url2,...>` - Blacklist specific URLs
    - `/clearurlblacklist <name>` - Clear URL blacklist
    - `/setheader <name> <text>` - Set header to remove
    - `/setfooter <name> <text>` - Set footer to remove
    - `/clearheaderfooter <name>` - Clear header/footer

    📝 Custom Text
    - `/setcustomheader <name> <text>` - Add custom header
    - `/setcustomfooter <name> <text>` - Add custom footer
    - `/clearcustomheaderfooter <name>` - Clear custom text

    🚫 Blocking
    - `/blocksentence <name> <sentence>` - Block a sentence
    - `/clearblocksentences <name>` - Clear blocked sentences
    - `/showblocksentences <name>` - Show blocked sentences
    """)
    await event.reply(commands)

@client.on(events.NewMessage(pattern='(?i)^/monitor$'))
async def monitor_pairs(event):
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply(render_emoji("⚠️ No forwarding pairs found."))
        return

    header = render_emoji("📊 Forwarding Monitor\n════════════════════\n")
    footer = render_emoji(f"\n════════════════════\n📥 Total Queued: {len(message_queue)}")
    report = []
    for pair_name, data in channel_mappings[user_id].items():
        stats = pair_stats.get(user_id, {}).get(pair_name, {
            'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
        })
        last_activity = stats['last_activity'] or 'N/A'
        if len(last_activity) > 20:
            last_activity = last_activity[:17] + "..."
        report.append(
            render_emoji(
                f"🔹 {pair_name}\n"
                f"   ↳ Route: {data['source']} → {data['destination']}\n"
                f"   ↳ Status: {'✅ Active' if data['active'] else '⏸️ Paused'}\n"
                f"   ↳ Stats: Fwd: {stats['forwarded']} | Edt: {stats['edited']} | Del: {stats['deleted']} | Blk: {stats['blocked']} | Que: {stats['queued']}\n"
                f"   ↳ Last: {last_activity}\n"
                f"───────────────"
            )
        )
    full_message = header + "\n".join(report) + footer
    await send_split_message(event, full_message)

@client.on(events.NewMessage(pattern=r'/setpair (\S+) (\S+) (\S+)(?: (yes|no))?'))
async def set_pair(event):
    pair_name, source, destination, remove_mentions = event.pattern_match.groups()
    user_id = str(event.sender_id)
    remove_mentions = remove_mentions == "yes"
    if user_id not in channel_mappings:
        channel_mappings[user_id] = {}
    if user_id not in pair_stats:
        pair_stats[user_id] = {}
    channel_mappings[user_id][pair_name] = {
        'source': source,
        'destination': destination,
        'active': True,
        'remove_mentions': remove_mentions,
        'blacklist': [],
        'block_urls': False,
        'blacklist_urls': [],
        'header_pattern': '',
        'footer_pattern': '',
        'custom_header': '',
        'custom_footer': '',
        'blocked_sentences': []
    }
    pair_stats[user_id][pair_name] = {'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None}
    save_mappings()
    await event.reply(render_emoji(f"✅ Pair '{pair_name}' Added\n{source} → {destination}\nMentions: {'❌' if remove_mentions else '✔️'}"))

@client.on(events.NewMessage(pattern=r'/blocksentence (\S+) (.+)'))
async def block_sentence(event):
    pair_name, sentence = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name].setdefault('blocked_sentences', []).append(sentence)
        save_mappings()
        await event.reply(render_emoji(f"🚫 Blocked Sentence Added for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/clearblocksentences (\S+)$'))
async def clear_block_sentences(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blocked_sentences'] = []
        save_mappings()
        await event.reply(render_emoji(f"🗑️ Blocked Sentences Cleared for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/showblocksentences (\S+)$'))
async def show_block_sentences(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        blocked_sentences = channel_mappings[user_id][pair_name].get('blocked_sentences', [])
        if blocked_sentences:
            sentences_list = "\n".join([f"• {s}" for s in blocked_sentences])
            await event.reply(render_emoji(f"📋 Blocked Sentences for '{pair_name}'\n{sentences_list}"))
        else:
            await event.reply(render_emoji(f"📋 No Blocked Sentences for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern=r'/addblacklist (\S+) (.+)'))
async def add_blacklist(event):
    pair_name, words = event.pattern_match.group(1), event.pattern_match.group(2).split(',')
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name].setdefault('blacklist', []).extend([w.strip() for w in words])
        channel_mappings[user_id][pair_name]['blacklist'] = list(set(channel_mappings[user_id][pair_name]['blacklist']))
        save_mappings()
        await event.reply(render_emoji(f"🚫 Added {len(words)} Word(s) to blacklist for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/clearblacklist (\S+)$'))
async def clear_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blacklist'] = []
        save_mappings()
        await event.reply(render_emoji(f"🗑️ Blacklist Cleared for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/showblacklist (\S+)$'))
async def show_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        blacklist = channel_mappings[user_id][pair_name].get('blacklist', [])
        if blacklist:
            words_list = ", ".join(blacklist)
            await event.reply(render_emoji(f"📋 Blacklist for '{pair_name}'\n{words_list}"))
        else:
            await event.reply(render_emoji(f"📋 No Blacklisted Words for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/toggleurlblock (\S+)$'))
async def toggle_url_block(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        current_status = channel_mappings[user_id][pair_name].get('block_urls', False)
        channel_mappings[user_id][pair_name]['block_urls'] = not current_status
        save_mappings()
        status = "ENABLED" if not current_status else "DISABLED"
        await event.reply(render_emoji(f"🔗 URL Blocking {status} for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern=r'/addurlblacklist (\S+) (.+)'))
async def add_url_blacklist(event):
    pair_name, urls = event.pattern_match.group(1), event.pattern_match.group(2).split(',')
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name].setdefault('blacklist_urls', []).extend([u.strip() for u in urls])
        channel_mappings[user_id][pair_name]['blacklist_urls'] = list(set(channel_mappings[user_id][pair_name]['blacklist_urls']))
        save_mappings()
        await event.reply(render_emoji(f"🚫 Added {len(urls)} URL(s) to blacklist for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/clearurlblacklist (\S+)$'))
async def clear_url_blacklist(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['blacklist_urls'] = []
        save_mappings()
        await event.reply(render_emoji(f"🗑️ URL Blacklist Cleared for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern=r'/setheader (\S+) (.+)'))
async def set_header(event):
    pair_name, pattern = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['header_pattern'] = pattern
        save_mappings()
        await event.reply(render_emoji(f"✂️ Header Set for '{pair_name}': '{pattern}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern=r'/setfooter (\S+) (.+)'))
async def set_footer(event):
    pair_name, pattern = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['footer_pattern'] = pattern
        save_mappings()
        await event.reply(render_emoji(f"✂️ Footer Set for '{pair_name}': '{pattern}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/clearheaderfooter (\S+)$'))
async def clear_header_footer(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['header_pattern'] = ''
        channel_mappings[user_id][pair_name]['footer_pattern'] = ''
        save_mappings()
        await event.reply(render_emoji(f"🗑️ Header/Footer Cleared for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern=r'/setcustomheader (\S+) (.+)'))
async def set_custom_header(event):
    pair_name, text = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_header'] = text
        save_mappings()
        await event.reply(render_emoji(f"📝 Custom Header Set for '{pair_name}': '{text}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern=r'/setcustomfooter (\S+) (.+)'))
async def set_custom_footer(event):
    pair_name, text = event.pattern_match.group(1), event.pattern_match.group(2)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_footer'] = text
        save_mappings()
        await event.reply(render_emoji(f"📝 Custom Footer Set for '{pair_name}': '{text}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/clearcustomheaderfooter (\S+)$'))
async def clear_custom_header_footer(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['custom_header'] = ''
        channel_mappings[user_id][pair_name]['custom_footer'] = ''
        save_mappings()
        await event.reply(render_emoji(f"🗑️ Custom Header/Footer Cleared for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/togglementions (\S+)$'))
async def toggle_mentions(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        current_status = channel_mappings[user_id][pair_name]['remove_mentions']
        channel_mappings[user_id][pair_name]['remove_mentions'] = not current_status
        save_mappings()
        status = "ENABLED" if not current_status else "DISABLED"
        await event.reply(render_emoji(f"🔄 Mention Removal {status} for '{pair_name}'"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/listpairs$'))
async def list_pairs(event):
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply(render_emoji("⚠️ No Forwarding Pairs Found"))
        return

    header = render_emoji("📋 Forwarding Pairs List\n════════════════════\n")
    pairs_list = []
    for name, data in channel_mappings[user_id].items():
        pairs_list.append(
            render_emoji(
                f"🔹 {name}\n"
                f"   ↳ Route: {data['source']} → {data['destination']}\n"
                f"   ↳ Active: {'✅' if data['active'] else '⏸️'}\n"
                f"   ↳ Mentions: {'❌' if data['remove_mentions'] else '✔️'}\n"
                f"   ↳ URLs: {'🚫' if data.get('block_urls', False) else '🔗'}\n"
                f"   ↳ URL BL: {len(data.get('blacklist_urls', []))}\n"
                f"   ↳ Header: '{data.get('header_pattern', '') or 'None'}'\n"
                f"   ↳ Footer: '{data.get('footer_pattern', '') or 'None'}'\n"
                f"   ↳ Custom H: '{data.get('custom_header', '') or 'None'}'\n"
                f"   ↳ Custom F: '{data.get('custom_footer', '') or 'None'}'\n"
                f"   ↳ Filters: BL: {len(data.get('blacklist', []))} | BS: {len(data.get('blocked_sentences', []))}\n"
                f"───────────────"
            )
        )
    full_message = header + "\n".join(pairs_list)
    await send_split_message(event, full_message)

@client.on(events.NewMessage(pattern='(?i)^/pausepair (\S+)$'))
async def pause_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = False
        save_mappings()
        await event.reply(render_emoji(f"⏸️ Pair '{pair_name}' Paused"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/startpair (\S+)$'))
async def start_pair(event):
    pair_name = event.pattern_match.group(1)
    user_id = str(event.sender_id)
    if user_id in channel_mappings and pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['active'] = True
        save_mappings()
        await event.reply(render_emoji(f"▶️ Pair '{pair_name}' Activated"))
    else:
        await event.reply(render_emoji("⚠️ Pair not found"))

@client.on(events.NewMessage(pattern='(?i)^/clearpairs$'))
async def clear_pairs(event):
    user_id = str(event.sender_id)
    if user_id in channel_mappings:
        channel_mappings[user_id] = {}
        pair_stats[user_id] = {}
        save_mappings()
        await event.reply(render_emoji("🗑️ All Pairs Cleared"))
    else:
        await event.reply(render_emoji("⚠️ No pairs to clear"))

@client.on(events.NewMessage)
async def forward_messages(event):
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    success = await forward_message_with_retry(event, mapping, user_id, pair_name)
                    if not success:
                        message_queue.append((event, mapping, user_id, pair_name))
                        pair_stats[user_id][pair_name]['queued'] += 1
                        logger.warning(f"Message queued for '{pair_name}'")
                except Exception as e:
                    logger.error(f"Error forwarding for '{pair_name}': {e}")
                    message_queue.append((event, mapping, user_id, pair_name))
                    pair_stats[user_id][pair_name]['queued'] += 1
                return

@client.on(events.MessageEdited)
async def handle_message_edit(event):
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    await edit_forwarded_message(event, mapping, user_id, pair_name)
                except Exception as e:
                    logger.error(f"Error editing for '{pair_name}': {e}")
                return

@client.on(events.MessageDeleted)
async def handle_message_deleted(event):
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['active'] and event.chat_id == int(mapping['source']):
                try:
                    for deleted_id in event.deleted_ids:
                        event.message.id = deleted_id
                        await delete_forwarded_message(event, mapping, user_id, pair_name)
                except Exception as e:
                    logger.error(f"Error handling deletion for '{pair_name}': {e}")
                return

async def check_connection_status():
    global is_connected
    while True:
        current_status = client.is_connected()
        if current_status and not is_connected:
            is_connected = True
            logger.info("Connection established, processing queue...")
            await process_message_queue()
        elif not current_status and is_connected:
            is_connected = False
            logger.warning("Connection lost, queuing messages...")
        await asyncio.sleep(5)

async def check_pair_inactivity():
    while True:
        await asyncio.sleep(300)
        if not is_connected or not NOTIFY_CHAT_ID:
            continue
        current_time = datetime.now()
        for user_id, pairs in channel_mappings.items():
            for pair_name, mapping in pairs.items():
                if not mapping['active']:
                    continue
                stats = pair_stats.get(user_id, {}).get(pair_name, {})
                last_activity_str = stats.get('last_activity')
                if not last_activity_str:
                    continue
                last_activity = datetime.fromisoformat(last_activity_str)
                inactivity_duration = (current_time - last_activity).total_seconds()
                if inactivity_duration > INACTIVITY_THRESHOLD:
                    await client.send_message(
                        NOTIFY_CHAT_ID,
                        render_emoji(f"⚠️ Inactivity Alert: Pair '{pair_name}' has had no activity for over {INACTIVITY_THRESHOLD // 60} minutes.")
                    )
                    pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()

async def send_periodic_report():
    while True:
        await asyncio.sleep(3600)
        if not is_connected or not MONITOR_CHAT_ID:
            continue
        for user_id in channel_mappings:
            header = render_emoji("📈 Hourly Report\n════════════════════\n")
            report = []
            total_queued = len(message_queue)
            for pair_name, data in channel_mappings[user_id].items():
                stats = pair_stats.get(user_id, {}).get(pair_name, {
                    'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
                })
                report.append(
                    render_emoji(
                        f"🔹 {pair_name}\n"
                        f"   ↳ Route: {data['source']} → {data['destination']}\n"
                        f"   ↳ Status: {'Active' if data['active'] else 'Paused'}\n"
                        f"   ↳ Fwd: {stats['forwarded']} | Edt: {stats['edited']} | Del: {stats['deleted']}\n"
                        f"   ↳ Blk: {stats['blocked']} | Que: {stats['queued']}\n"
                        f"───────────────"
                    )
                )
            full_message = header + "\n".join(report) + render_emoji(f"\n📥 Queued: {total_queued}")
            try:
                await client.send_message(MONITOR_CHAT_ID, full_message)
                logger.info("Sent periodic report")
            except Exception as e:
                logger.error(f"Error sending report: {e}")

async def main():
    load_mappings()
    asyncio.create_task(check_connection_status())
    asyncio.create_task(send_periodic_report())
    asyncio.create_task(check_pair_inactivity())
    logger.info("🚀 Bot is starting...")

    try:
        await client.start()
        if not await client.is_user_authorized():
            phone = input("Please enter your phone (or bot token): ")
            await client.start(phone=phone)
            code = input("Please enter the verification code you received: ")
            await client.sign_in(phone=phone, code=code)

        global is_connected, MONITOR_CHAT_ID, NOTIFY_CHAT_ID
        is_connected = client.is_connected()
        MONITOR_CHAT_ID = (await client.get_me()).id
        NOTIFY_CHAT_ID = MONITOR_CHAT_ID

        if is_connected:
            logger.info("Initial connection established")
        else:
            logger.warning("Initial connection not established")

        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Bot is shutting down...")
        save_mappings()

if __name__ == "__main__":
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
