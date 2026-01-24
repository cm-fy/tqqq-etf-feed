#!/usr/bin/env python3
"""
Fetch TQQQ ETF data and generate Atom (docs/feed.atom) and RSS2 (docs/feed.rss).
This mirrors the ocgn-stock-feed implementation and behavior but targets the
TQQQ ticker and uses ET (America/New_York) for timestamps and the trading window.
"""
import os
import shutil
import datetime as dt
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET
from xml.dom import minidom
from email.utils import format_datetime

import yfinance as yf
import pandas as pd

# Configuration
FEED_URL = "https://cm-fy.github.io/tqqq-etf-feed/feed.atom"
FEED_RSS_URL = "https://cm-fy.github.io/tqqq-etf-feed/feed.rss"
FEED_ICON = "https://cm-fy.github.io/tqqq-etf-feed/TQQQ.png"
FEED_HOMEPAGE = "https://cm-fy.github.io/tqqq-etf-feed/"
FEED_TITLE = "TQQQ ETF Price Feed"
FEED_SUBTITLE = "Near-real-time TQQQ (ProShares UltraPro QQQ) price updates."
FEED_AUTHOR = "TQQQ Feed Bot"
SYMBOL = "TQQQ"

# Full-window parameters (ET)
START_HOUR = 4
END_HOUR = 21  # inclusive end at 21:00 ET
RESAMPLE_FREQ = "5min"  # 5-minute buckets
ATOM_NS = "http://www.w3.org/2005/Atom"
ET.register_namespace('', ATOM_NS)

ET_ZONE = ZoneInfo('America/New_York')
UTC = ZoneInfo('UTC')

# RSS/Atom behavior
MAX_RSS_ITEMS = 50  # limit RSS/Atom items emitted
# Behavior flags (can be set via environment in the workflow)
# Set EMIT_FALLBACK=0 to disable the fallback that emits full window when series is flat
EMIT_FALLBACK = os.environ.get('EMIT_FALLBACK', '1').lower() in ('1', 'true', 'yes')


def fetch_tqqq_data():
    try:
        t = yf.Ticker(SYMBOL)
        # Prefer get_info() because it includes `marketState`, `preMarketPrice`, and timestamps.
        try:
            info = t.get_info()
        except Exception:
            info = t.info if hasattr(t, 'info') else {}

        # Candle history is sometimes stale during extended hours; we can supplement with quote fields from `info`.
        hist = t.history(period="2d", interval="1m", prepost=True)

        # Try to get the freshest price from fast_info and last 1m candle
        fast_info = getattr(t, 'fast_info', {}) or {}
        freshest = []
        # 1. Yahoo quote fields
        for k in [
            ('preMarketPrice', 'preMarketTime'),
            ('postMarketPrice', 'postMarketTime'),
            ('regularMarketPrice', 'regularMarketTime'),
        ]:
            price = info.get(k[0])
            ts = info.get(k[1])
            if price is not None and ts is not None:
                try:
                    price_f = float(price)
                    ts_i = int(ts)
                    freshest.append(('quote', k[0], price_f, ts_i))
                except Exception:
                    pass
        # 2. fast_info last_price
        fi_price = fast_info.get('last_price')
        fi_ts = None
        if fi_price is not None:
            # No timestamp, but use now
            freshest.append(('fast_info', 'last_price', float(fi_price), int(dt.datetime.now(dt.timezone.utc).timestamp())))
        # 3. Last 1m candle
        if hist is not None and not hist.empty:
            last_idx = hist.index[-1]
            last_price = hist.iloc[-1]['Close'] if 'Close' in hist.columns else None
            if last_price is not None:
                freshest.append(('history', 'Close', float(last_price), int(last_idx.timestamp())))
        # Sort by timestamp descending
        freshest = sorted(freshest, key=lambda x: x[3], reverse=True)
        # Print all sources for debug
        print("FRESH_PRICE_SOURCES:")
        for src in freshest:
            print(f"  {src}")
        return info, hist, freshest
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {}, pd.DataFrame(), []


def pick_quote_price_and_time(info: dict):
    """Pick the best available quote price+time from Yahoo quote fields.

    Returns (price, timestamp_utc_seconds, source_key) or (None, None, None).
    """
    if not isinstance(info, dict) or not info:
        return None, None, None

    market_state = info.get('marketState')
    candidates = []

    # Prefer session-specific fields when marketState suggests them.
    if market_state == 'PRE':
        candidates.extend([
            ('preMarketPrice', 'preMarketTime'),
            ('regularMarketPrice', 'regularMarketTime'),
        ])
    elif market_state == 'POST':
        candidates.extend([
            ('postMarketPrice', 'postMarketTime'),
            ('regularMarketPrice', 'regularMarketTime'),
        ])
    else:
        candidates.extend([
            ('regularMarketPrice', 'regularMarketTime'),
            ('preMarketPrice', 'preMarketTime'),
            ('postMarketPrice', 'postMarketTime'),
        ])

    for price_key, time_key in candidates:
        price = info.get(price_key)
        ts = info.get(time_key)
        if price is None or ts is None:
            continue
        try:
            price_f = float(price)
            ts_i = int(ts)
        except Exception:
            continue
        return price_f, ts_i, price_key

    return None, None, None


def build_full_window_index(date_et: dt.date):
    start = dt.datetime.combine(date_et, dt.time(START_HOUR, 0), tzinfo=ET_ZONE)
    end = dt.datetime.combine(date_et, dt.time(END_HOUR, 0), tzinfo=ET_ZONE)
    return pd.date_range(start=start, end=end, freq=RESAMPLE_FREQ)


def floor_to_5min(ts: dt.datetime) -> dt.datetime:
    # Floor a timezone-aware datetime to the nearest lower 5-minute boundary.
    if ts.tzinfo is None:
        raise ValueError('floor_to_5min requires a timezone-aware datetime')
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


def price_series_from_hist(hist_df: pd.DataFrame) -> pd.Series:
    if hist_df is None or hist_df.empty:
        return pd.Series(dtype=float)

    df = hist_df.copy()
    if df.index.tz is None:
        df = df.tz_localize('UTC')
    df = df.tz_convert(ET_ZONE)

    if 'Close' in df.columns:
        return df['Close']

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if numeric_cols:
        return df[numeric_cols[0]]

    return pd.Series(dtype=float)


def generate_atom_and_rss(info, hist, freshest):
    # Prepare price series
    price_series = price_series_from_hist(hist)

    # Determine today's window and restrict to <= now (avoid generating future timestamps)
    now_et = dt.datetime.now(ET_ZONE)
    target_date = now_et.date()

    window_start = dt.datetime.combine(target_date, dt.time(START_HOUR, 0), tzinfo=ET_ZONE)
    window_end = dt.datetime.combine(target_date, dt.time(END_HOUR, 0), tzinfo=ET_ZONE)
    effective_end = min(window_end, floor_to_5min(now_et))
    if effective_end < window_start:
        # Before the window starts: keep index empty so we don't emit misleading timestamps.
        full_index = pd.DatetimeIndex([])
    else:
        full_index = pd.date_range(start=window_start, end=effective_end, freq=RESAMPLE_FREQ)

    # Discard historic data from previous days so we don't accidentally carry
    # yesterday's after-hours price into today's pre-market window.
    try:
        if len(full_index) > 0:
            start_of_window = full_index[0]
            price_series = price_series[price_series.index >= start_of_window]
    except Exception:
        pass

    # Always inject the freshest available price (from any source)
    if freshest and len(full_index) > 0:
        src, src_key, price, ts = freshest[0]
        quote_dt_et = dt.datetime.fromtimestamp(ts, tz=UTC).astimezone(ET_ZONE)
        if quote_dt_et >= full_index[0]:
            quote_point = pd.Series([price], index=pd.DatetimeIndex([quote_dt_et]))
            price_series = pd.concat([price_series, quote_point]).sort_index()
            price_series = price_series[~price_series.index.duplicated(keep='last')]

    # For previous close, attempt to find last close prior to start of window
    previous_close = None
    try:
        if hist is not None and not hist.empty:
            hist_utc = hist.copy()
            if hist_utc.index.tz is None:
                hist_utc = hist_utc.tz_localize('UTC')
            hist_et = hist_utc.tz_convert(ET_ZONE)
            start_et = dt.datetime.combine(target_date, dt.time(START_HOUR, 0), tzinfo=ET_ZONE)
            prev_vals = hist_et[hist_et.index < start_et]
            if not prev_vals.empty:
                if 'Close' in prev_vals.columns:
                    previous_close = prev_vals['Close'].iloc[-1]
                else:
                    numeric_cols = [c for c in prev_vals.columns if pd.api.types.is_numeric_dtype(prev_vals[c])]
                    if numeric_cols:
                        previous_close = prev_vals[numeric_cols[0]].iloc[-1]
    except Exception:
        previous_close = None

    # Resample to 5-minute using last available price in each bucket
    if not price_series.empty:
        price_5m = price_series.resample(RESAMPLE_FREQ).last().ffill()
    else:
        # If there's no intraday data for today but we have a previous close,
        # populate the full window with the previous close so pre-market readers
        # see a sensible price rather than an empty feed.
        if previous_close is not None:
            price_5m = pd.Series([previous_close] * len(full_index), index=full_index)
        else:
            price_5m = pd.Series(dtype=float)

    # Reindex price_5m to the full window (index in ET)
    if len(full_index) == 0:
        price_5m = pd.Series(dtype=float)
    elif not price_5m.empty:
        price_5m = price_5m.reindex(full_index, method='ffill')
    else:
        price_5m = pd.Series([None] * len(full_index), index=full_index)

    # Build Atom feed
    feed = ET.Element(ET.QName(ATOM_NS, 'feed'))
    title = ET.SubElement(feed, ET.QName(ATOM_NS, 'title'))
    title.text = FEED_TITLE
    subtitle = ET.SubElement(feed, ET.QName(ATOM_NS, 'subtitle'))
    subtitle.text = FEED_SUBTITLE
    link_self = ET.SubElement(feed, ET.QName(ATOM_NS, 'link'))
    link_self.set('href', FEED_URL)
    link_self.set('rel', 'self')
    link_alt = ET.SubElement(feed, ET.QName(ATOM_NS, 'link'))
    link_alt.set('href', FEED_HOMEPAGE)
    link_alt.set('rel', 'alternate')
    link_alt.set('type', 'text/html')
    feed_id = ET.SubElement(feed, ET.QName(ATOM_NS, 'id'))
    feed_id.text = FEED_URL
    updated = ET.SubElement(feed, ET.QName(ATOM_NS, 'updated'))
    updated.text = now_et.isoformat()
    author = ET.SubElement(feed, ET.QName(ATOM_NS, 'author'))
    name = ET.SubElement(author, ET.QName(ATOM_NS, 'name'))
    name.text = FEED_AUTHOR
    generator = ET.SubElement(feed, ET.QName(ATOM_NS, 'generator'))
    generator.text = 'fetch_tqqq_feed.py (custom)'
    icon_el = ET.SubElement(feed, ET.QName(ATOM_NS, 'icon'))
    icon_el.text = FEED_ICON
    logo_el = ET.SubElement(feed, ET.QName(ATOM_NS, 'logo'))
    logo_el.text = FEED_ICON

    # Build RSS channel items list
    rss_items = []


    # Build a filtered list of timestamps where the price changed (avoid emitting long series of identical prices)
    emitted = []
    emitted = []
    last_price_rounded = None
    for ts in full_index:
        price = price_5m.get(ts, None)
        if price is None or pd.isna(price):
            continue
        # Compare prices rounded to cents to avoid tiny float differences causing repeats
        price_r = round(float(price), 2)
        if last_price_rounded is None or price_r != last_price_rounded:
            emitted.append((ts, price))
            last_price_rounded = price_r

    # Keep only the most recent MAX_RSS_ITEMS
    emitted = emitted[-MAX_RSS_ITEMS:]

    # Precompute a mapping from timestamp to price for quick lookup
    price_lookup = {ts: price_5m.get(ts, None) for ts in full_index}

    # Fallback: if only one or zero emitted items but the full window contains
    # multiple non-NaN slots (flat market / holiday / pre-market), emit the
    # last MAX_RSS_ITEMS slots so readers still get a sensible feed.
    if EMIT_FALLBACK and len(emitted) <= 1:
        nonempty = [(ts, price_lookup.get(ts)) for ts in full_index if price_lookup.get(ts) is not None and not pd.isna(price_lookup.get(ts))]
        if len(nonempty) > 1:
            # compress consecutive identical prices (rounded to cents) so readers don't get many identical items
            compressed = []
            last_r = None
            for ts, p in nonempty:
                pr = round(float(p), 2)
                if last_r is None or pr != last_r:
                    compressed.append((ts, p))
                    last_r = pr
            emitted = compressed[-MAX_RSS_ITEMS:]

    # Build Atom entries and RSS items from emitted list (newest-first in feed)
    for ts, price in reversed(emitted):
        # --- Atom entry (unchanged) ---
        entry = ET.SubElement(feed, ET.QName(ATOM_NS, 'entry'))
        title_entry = ET.SubElement(entry, ET.QName(ATOM_NS, 'title'))
        price_text = f"{SYMBOL}: ${price:.2f}"
        title_entry.text = price_text
        link = ET.SubElement(entry, ET.QName(ATOM_NS, 'link'))
        link.set('href', f"https://finance.yahoo.com/quote/{SYMBOL}")
        link.set('rel', 'alternate')
        link.set('type', 'text/html')
        entry_id = ET.SubElement(entry, ET.QName(ATOM_NS, 'id'))
        entry_id.text = f"{SYMBOL.lower()}-{ts.strftime('%Y%m%d-%H%M')}"
        # published/updated in ET
        published = ET.SubElement(entry, ET.QName(ATOM_NS, 'published'))
        published.text = ts.isoformat()
        entry_updated = ET.SubElement(entry, ET.QName(ATOM_NS, 'updated'))
        entry_updated.text = ts.isoformat()
        entry_author = ET.SubElement(entry, ET.QName(ATOM_NS, 'author'))
        entry_author_name = ET.SubElement(entry_author, ET.QName(ATOM_NS, 'name'))
        entry_author_name.text = FEED_AUTHOR
        summary = ET.SubElement(entry, ET.QName(ATOM_NS, 'summary'))
        if previous_close is not None:
            change = price - previous_close
            pct = (change / previous_close * 100) if previous_close else 0
            summary.text = f"{SYMBOL} {price:.2f} ({change:+.2f}, {pct:+.2f}%) at {ts.strftime('%H:%M %Z')}"
        else:
            summary.text = f"{SYMBOL} {price:.2f} at {ts.strftime('%H:%M %Z')}"
        content = ET.SubElement(entry, ET.QName(ATOM_NS, 'content'))
        content.set('type', 'html')
        content_html = "<div>\n"
        content_html += f"<h2>{SYMBOL} Stock Price Update</h2>\n"
        content_html += f"<p><strong>Price:</strong> ${price:.2f}</p>\n"
        if previous_close is not None:
            content_html += f"<p><strong>Previous Close:</strong> ${previous_close:.2f}</p>\n"
        content_html += f"<p><strong>Timestamp (ET):</strong> {ts.strftime('%Y-%m-%d %H:%M %Z')}</p>\n"
        content_html += "</div>"
        content.text = content_html

        # --- RSS item (add 1h diff) ---
        pubdate_et = ts.astimezone(ET_ZONE)

        # Find price from 1 hour ago (if available)
        ts_1h_ago = ts - pd.Timedelta(hours=1)
        price_1h_ago = None
        # Find the closest earlier or equal timestamp in full_index
        idxs = [i for i, t in enumerate(full_index) if t <= ts_1h_ago]
        if idxs:
            ts_prev = full_index[max(idxs)]
            price_1h_ago = price_lookup.get(ts_prev, None)
        # Compute diff and percent
        diff_str = ""
        if price_1h_ago is not None and not pd.isna(price_1h_ago):
            diff = price - price_1h_ago
            pct = (diff / price_1h_ago * 100) if price_1h_ago else 0
            diff_str = f" ({diff:+.2f}, {pct:+.2f}% vs 1h ago)"

        # Add published time to title for clarity
        published_str = ts.strftime('%H:%M %Z')
        rss_title = f"{SYMBOL}: ${price:.2f}{diff_str} [{published_str}]"

        # Add published time to description as well
        rss_desc = content_html + f"<br/><small>Published: {published_str}</small>"

        rss_items.append({
            'title': rss_title,
            'link': f"https://finance.yahoo.com/quote/{SYMBOL}",
            'guid': entry_id.text,
            'pubDate': format_datetime(pubdate_et),
            'description': rss_desc
        })

    # Print emitted count + latest sources for workflow logs
    try:
        last_hist_et = None
        if hist is not None and not hist.empty:
            hi = hist.copy()
            if hi.index.tz is None:
                hi = hi.tz_localize('UTC')
            last_hist_et = hi.index.max().tz_convert(ET_ZONE)

        freshest_dbg = None
        if freshest:
            src, src_key, price, ts = freshest[0]
            freshest_dbg = f"{src}:{src_key}={price} @ {dt.datetime.fromtimestamp(int(ts), tz=UTC).astimezone(ET_ZONE)}"

        print(
            f"EMIT_FALLBACK={EMIT_FALLBACK}; EMITTED_COUNT={len(emitted)}; "
            f"MARKET_STATE={info.get('marketState')}; "
            f"WINDOW_END={full_index[-1] if len(full_index) else None}; "
            f"LAST_HIST={last_hist_et}; FRESHEST={freshest_dbg}"
        )
    except Exception:
        print(f"EMIT_FALLBACK={EMIT_FALLBACK}; EMITTED_COUNT={len(emitted)}")
    return feed, rss_items, now_et


def prettify_xml(elem):
    rough_string = ET.tostring(elem, encoding='utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding='utf-8').decode('utf-8')


def write_rss(rss_items, now_et):
    channel_title = FEED_TITLE
    channel_link = FEED_HOMEPAGE
    channel_desc = FEED_SUBTITLE
    rss_parts = []
    rss_parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    rss_parts.append('<rss version="2.0">')
    rss_parts.append('<channel>')
    rss_parts.append(f"<title>{channel_title}</title>")
    rss_parts.append(f"<link>{channel_link}</link>")
    rss_parts.append(f"<description>{channel_desc}</description>")
    rss_parts.append(f"<lastBuildDate>{format_datetime(now_et)}</lastBuildDate>")
    rss_parts.append(f"<generator>fetch_tqqq_feed.py (custom)</generator>")
    rss_parts.append(f"<image><url>{FEED_ICON}</url><title>{channel_title}</title><link>{channel_link}</link></image>")

    for it in rss_items:
        rss_parts.append('<item>')
        rss_parts.append(f"<title><![CDATA[{it['title']}]]></title>")
        rss_parts.append(f"<link>{it['link']}</link>")
        rss_parts.append(f"<guid>{it['guid']}</guid>")
        rss_parts.append(f"<pubDate>{it['pubDate']}</pubDate>")
        rss_parts.append(f"<description><![CDATA[{it['description']}]]></description>")
        rss_parts.append('</item>')

    rss_parts.append('</channel>')
    rss_parts.append('</rss>')
    return '\n'.join(rss_parts)


def ensure_icon_is_deployed():
    src = 'TQQQ.png'
    dst_dir = 'docs'
    dst = os.path.join(dst_dir, 'TQQQ.png')
    try:
        if os.path.exists(src):
            os.makedirs(dst_dir, exist_ok=True)
            shutil.copyfile(src, dst)
            print(f"Copied {src} -> {dst} so it will be deployed to Pages")
        else:
            print(f"Note: {src} not found in repo root; skipping copy. If you want an icon, add TQQQ.png at repo root or put the image in docs/.")
    except Exception as e:
        print(f"Warning: could not copy icon file: {e}")


def main():
    try:
        print("Fetching TQQQ data...")
        info, hist, freshest = fetch_tqqq_data()
        print("Generating Atom and RSS feeds...")
        feed, rss_items, now_et = generate_atom_and_rss(info, hist, freshest)
        print("Writing feed files...")
        feed_xml = prettify_xml(feed)
        os.makedirs('docs', exist_ok=True)
        ensure_icon_is_deployed()
        with open('docs/feed.atom', 'w', encoding='utf-8') as f:
            f.write(feed_xml)
        rss_text = write_rss(rss_items, now_et)
        with open('docs/feed.rss', 'w', encoding='utf-8') as f:
            f.write(rss_text)
        index_html = f"""<!DOCTYPE html>
<html lang=\"en\">\n<head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{FEED_TITLE}</title><link rel=\"icon\" type=\"image/png\" href=\"TQQQ.png\" /></head>\n<body><h1>{FEED_TITLE}</h1><p>{FEED_SUBTITLE}</p><p><a href=\"feed.atom\">Atom feed</a> | <a href=\"feed.rss\">RSS2 feed</a></p></body></html>"""
        with open('docs/index.html', 'w', encoding='utf-8') as f:
            f.write(index_html)
        print('Wrote docs/feed.atom, docs/feed.rss and docs/index.html')
    except Exception as e:
        print('Warning: feed generation failed but will not crash workflow:', e)


if __name__ == '__main__':
    main()
