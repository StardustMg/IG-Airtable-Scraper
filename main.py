import json
import requests
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

def load_config():
    """Load Airtable credentials from environment variables only."""
    api_key = os.getenv("AIRTABLE_API_KEY")
    base_id = os.getenv("AIRTABLE_BASE_ID")

    if not api_key or not base_id:
        raise RuntimeError("Missing environment variables: AIRTABLE_API_KEY or AIRTABLE_BASE_ID")

    logging.info("Loaded Airtable config from environment.")
    return {
        "api_key": api_key,
        "base_id": base_id
    }

def fetch_table_records(api_key, base_id, table_name, view=None):
    """
    Fetch all records from an Airtable table (with optional view filter).
    Returns a list of record dicts.
    """
    logging.info(f"Fetching records from table '{table_name}' (view={view})")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    url = f"https://api.airtable.com/v0/{base_id}/{requests.utils.quote(table_name)}"
    params = {}
    if view:
        params["view"] = view

    all_records = []
    offset = None
    while True:
        if offset:
            params["offset"] = offset
        logging.debug(f"Requesting page with offset={offset}")
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("records", [])
        logging.info(f"Fetched {len(records)} records from this page")
        all_records.extend(records)
        offset = data.get("offset")
        if not offset:
            break

    logging.info(f"Total records fetched from '{table_name}': {len(all_records)}")
    return all_records

def records_to_dataframe(records):
    """
    Convert a list of Airtable records into a pandas DataFrame.
    Each record’s 'fields' dict becomes one row.
    """
    logging.info(f"Converting {len(records)} records to DataFrame")
    rows = [rec.get("fields", {}) for rec in records]
    df = pd.DataFrame(rows)
    logging.info("Conversion to DataFrame complete")
    return df

def fetch_agency_accounts(api_key, base_id, view=None):
    """
    Fetches all records from the "🤩 Agency Accounts" table and returns
    a DataFrame with columns 'username' and 'followers'.
    """
    logging.info("Fetching agency accounts DataFrame")
    records = fetch_table_records(api_key, base_id, "🤩 Agency Accounts", view=view)
    df = records_to_dataframe(records)
    df = df.rename(columns={"📸 Username": "username", "Followers": "followers"})
    result = df[["username", "followers"]]
    logging.info(f"Agency accounts DataFrame prepared with {len(result)} rows")
    return result

def update_agency_account_stats(api_key, base_id, rapidapi_key):
    """
    For each record in "🤩 Agency Accounts", scrapes Instagram user info via RapidAPI,
    then updates in Airtable. Logs the entire JSON whenever “data” is missing or malformed.
    """
    import requests
    import logging
    from urllib.parse import quote

    logging.info("Starting update_agency_account_stats")
    airtable_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    records = fetch_table_records(api_key, base_id, "🤩 Agency Accounts")
    logging.info(f"Fetched {len(records)} agency accounts to process")

    for rec in records:
        record_id     = rec["id"]
        flds          = rec.get("fields", {})
        username      = flds.get("📸 Username")
        old_followers = flds.get("Followers", 0)

        logging.info(f"[{record_id}] Processing account '{username}'")
        if not username:
            logging.warning(f"[{record_id}] No username; skipping")
            continue

        try:
            # 1) Call RapidAPI
            url_info = "https://real-time-instagram-scraper-api1.p.rapidapi.com/v1/user_info"
            params   = {"username_or_id": username}
            headers_rapid = {
                "x-rapidapi-key": rapidapi_key,
                "x-rapidapi-host": "real-time-instagram-scraper-api1.p.rapidapi.com"
            }
            resp = requests.get(url_info, headers=headers_rapid, params=params, timeout=15)
            logging.debug(f"[{record_id}] RapidAPI status: {resp.status_code}")
            resp.raise_for_status()

            body = resp.json()
            data = body.get("data")
            if not isinstance(data, dict):
                # *** new debug: log full body ***
                logging.error(f"[{record_id}] Unexpected RapidAPI response for '{username}': {body}")
                raise ValueError("missing or malformed 'data'")

            # 2) Extract fields
            pic_url    = data["hd_profile_pic_url_info"]["url"]
            full_name  = data["full_name"]
            bio        = data["biography"]
            bio_link   = data["external_url"]
            followers  = data["follower_count"]
            following  = data["following_count"]
            posts      = data["media_count"]
            diff       = followers - old_followers

            logging.debug(
                f"[{record_id}] Parsed data: followers={followers}, following={following}, posts={posts}"
            )

            # 3) Patch Airtable
            update_fields = {
                "🖼️ Profile picture": [{"url": pic_url}],
                "🤙 Name": full_name,
                "📓 Bio": bio,
                "📓 Bio link": bio_link,
                "Followers": followers,
                "Followers (+ L24H)": diff,
                "Following": following,
                "Posts": posts
            }
            update_url = (
                f"https://api.airtable.com/v0/{base_id}/"
                f"{requests.utils.quote('🤩 Agency Accounts')}"
            )
            payload = {"records": [{"id": record_id, "fields": update_fields}]}
            logging.debug(f"[{record_id}] PATCH Airtable: {update_fields}")
            r = requests.patch(update_url, json=payload, headers=airtable_headers, timeout=15)
            logging.debug(f"[{record_id}] Airtable response: {r.status_code}")
            r.raise_for_status()

            logging.info(f"[{record_id}] Successfully updated '{username}'")

        except requests.exceptions.Timeout as e:
            logging.error(f"[{record_id}] Timeout when fetching '{username}': {e}")
        except requests.exceptions.HTTPError as e:
            logging.error(f"[{record_id}] HTTP error for '{username}': {e}; body={e.response.text}")
        except (KeyError, TypeError, ValueError) as e:
            logging.error(f"[{record_id}] Data error for '{username}': {e}")
        # continue regardless of errors
    logging.info("Completed update_agency_account_stats")



def scrape_agency_reels(api_key, base_id, rapidapi_key):
    """
    For each record in "🤩 Agency Accounts", fetch all reels via RapidAPI (paginated),
    then insert each reel from the past 30 days into "🎥 Agency Reels",
    skipping any whose 🤖 Reel ID is already present or whose JSON is malformed.
    """
    logging.info("Starting scrape_agency_reels")
    from urllib.parse import quote
    import datetime

    airtable_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.airtable.com/v0/{base_id}"
    reels_url = "https://real-time-instagram-scraper-api1.p.rapidapi.com/v1/user_reels"
    rapid_headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "real-time-instagram-scraper-api1.p.rapidapi.com"
    }

    existing = fetch_table_records(api_key, base_id, "🎥 Agency Reels")
    existing_ids = {
        rec["fields"].get("🤖 Reel ID")
        for rec in existing
        if rec.get("fields", {}).get("🤖 Reel ID")
    }
    logging.info(f"Loaded {len(existing_ids)} existing reel IDs")

    accounts = fetch_table_records(api_key, base_id, "🤩 Agency Accounts")
    logging.info(f"Processing {len(accounts)} agency accounts")
    cutoff = datetime.datetime.now() - datetime.timedelta(days=30)

    for acc in accounts:
        record_id = acc["id"]
        f = acc.get("fields", {})
        username = f.get("📸 Username")
        snapshot = f.get("Followers", 0)
        if not username:
            logging.warning(f"Skipping account with missing username (record {record_id})")
            continue

        logging.info(f"Scraping reels for @{username}")
        max_id = None

        try:
            while True:
                params = {"username_or_id": username}
                if max_id:
                    params["max_id"] = max_id

                resp = requests.get(reels_url, headers=rapid_headers, params=params)
                resp.raise_for_status()
                body = resp.json()
                data = body.get("data")
                if not isinstance(data, dict):
                    raise ValueError("malformed data")

                items = data.get("items", [])
                logging.info(f"Fetched {len(items)} items from RapidAPI page (max_id={max_id})")

                for item in items:
                    try:
                        m = item["media"]
                        reel_id = m.get("code")
                        if not reel_id:
                            logging.debug("Skipping item with no code")
                            continue
                        if reel_id in existing_ids:
                            logging.debug(f"Skipping duplicate reel {reel_id}")
                            continue

                        posted = datetime.datetime.fromtimestamp(m["taken_at"])
                        if posted < cutoff:
                            logging.debug(f"Skipping old reel {reel_id} posted on {posted}")
                            continue

                        cap = m.get("caption") or {}
                        caption_text = cap.get("text", "")

                        vid = max(
                            m.get("video_versions", []),
                            key=lambda v: v.get("height", 0),
                            default={}
                        )
                        download_link = vid.get("url", "")
                        logging.debug(f"Prepared reel {reel_id}: link={download_link}")

                        record = {
                            "🔒 Account": [record_id],
                            "🔢 Followers Snapshot": snapshot,
                            "📒 Caption": caption_text,
                            "💬 Comment count": m.get("comment_count", 0),
                            "👀 Views": m.get("play_count", 0),
                            "👍 Like count": m.get("like_count", 0),
                            "🗓️ Date of posting": posted.date().isoformat(),
                            "🤖 Reel ID": reel_id,
                            "⬇️ Download link": download_link
                        }

                        create_url = f"{base_url}/{quote('🎥 Agency Reels')}"
                        payload = {"records": [{"fields": record}]}
                        r = requests.post(create_url, json=payload, headers=airtable_headers)
                        r.raise_for_status()
                        existing_ids.add(reel_id)
                        logging.info(f"Inserted reel {reel_id} for @{username}")

                    except (KeyError, TypeError, ValueError) as e:
                        logging.warning(f"Skipping malformed reel for @{username}: {e}")
                        continue

                paging = data.get("paging_info", {})
                new_max = body.get("max_id")
                if not paging.get("more_available") or new_max == max_id:
                    logging.info("No more pages or max_id unchanged, breaking pagination")
                    break
                max_id = new_max

        except (requests.RequestException, ValueError) as e:
            logging.error(f"Skipping account '{username}' due to error: {e}")
            continue

    logging.info("Completed scrape_agency_reels")

def calculate_and_update_account_stats(api_key, base_id):
    """
    1) Updates each “🤩 Agency Accounts” record with Total Views, Posts (1D/3D/7D) and Avg view/video.
    2) Updates each “🎥 Agency Reels” record with Virality score,
       auto-detecting the actual field name in Airtable so we don’t hard-code a mismatched emoji.
    """
    logging.info("Starting calculate_and_update_account_stats")
    from urllib.parse import quote
    import datetime

    base_url = f"https://api.airtable.com/v0/{base_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    accounts = fetch_table_records(api_key, base_id, "🤩 Agency Accounts")
    reels    = fetch_table_records(api_key, base_id, "🎥 Agency Reels")
    logging.info(f"Fetched {len(accounts)} agency accounts and {len(reels)} reels")

    # detect real Virality field
    vir_field = None
    if reels:
        sample = reels[0].get("fields", {})
        for name in sample:
            if "Virality" in name:
                vir_field = name
                break
    if not vir_field:
        vir_field = "Virality score"
    logging.info(f"Using virality field '{vir_field}'")

    # group reels
    reels_by_account = {}
    for reel in reels:
        for acc_id in reel.get("fields", {}).get("🔒 Account", []):
            reels_by_account.setdefault(acc_id, []).append(reel)

    today = datetime.date.today()
    account_avg = {}

    # update accounts
    for acc in accounts:
        acc_id = acc["id"]
        recs   = reels_by_account.get(acc_id, [])
        total_views = sum(r["fields"].get("👀 Views", 0) for r in recs)
        n_reels     = len(recs)

        def days_old(r):
            return (today - datetime.date.fromisoformat(r["fields"]["🗓️ Date of posting"])).days

        posts_1d = sum(1 for r in recs if days_old(r) == 0)
        posts_3d = sum(1 for r in recs if days_old(r) < 3)
        posts_7d = sum(1 for r in recs if days_old(r) < 7)

        avg = (total_views / n_reels) if n_reels else 0
        account_avg[acc_id] = avg
        logging.info(f"Account {acc_id}: total_views={total_views}, posts_1d={posts_1d}, avg={avg:.2f}")

        acct_fields = {
            "Total Views": total_views,
            "Posts (1D)":  posts_1d,
            "Posts (3D)":  posts_3d,
            "Posts (7D)":  posts_7d,
            "🎯 Avg view / video (L30D)": avg
        }
        url     = f"{base_url}/{quote('🤩 Agency Accounts')}"
        payload = {"records": [{"id": acc_id, "fields": acct_fields}]}
        requests.patch(url, json=payload, headers=headers).raise_for_status()

    # update reel virality
    for reel in reels:
        rid   = reel["id"]
        flds  = reel.get("fields", {})
        accs  = flds.get("🔒 Account", [])
        if not accs:
            continue
        avg   = account_avg.get(accs[0], 0)
        views = flds.get("👀 Views", 0)
        virality = ((views - avg) / avg) if avg else 0
        logging.debug(f"Reel {rid}: views={views}, avg={avg:.2f}, virality={virality:.4f}")

        reel_fields = {vir_field: virality}
        url     = f"{base_url}/{quote('🎥 Agency Reels')}"
        payload = {"records": [{"id": rid, "fields": reel_fields}]}
        try:
            r = requests.patch(url, json=payload, headers=headers)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if r.status_code == 422 and "UNKNOWN_FIELD_NAME" in r.text:
                logging.error(f"Virality field not found: {vir_field}")
                break
            else:
                logging.error(f"Error updating reel {rid}: {r.status_code} — {r.text}")
                continue

    logging.info("Completed calculate_and_update_account_stats")

def update_competitor_accounts(api_key, base_id, rapidapi_key):
    """
    For each record in "🎯 Competitor Accounts", fetches Instagram user_info via RapidAPI
    and updates Airtable fields.
    """
    logging.info("Starting update_competitor_accounts")
    from urllib.parse import quote

    airtable_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.airtable.com/v0/{base_id}"
    endpoint = "https://real-time-instagram-scraper-api1.p.rapidapi.com/v1/user_info"
    rapid_headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "real-time-instagram-scraper-api1.p.rapidapi.com"
    }

    records = fetch_table_records(api_key, base_id, "🎯 Competitor Accounts")
    logging.info(f"Fetched {len(records)} competitor accounts")

    for rec in records:
        rec_id = rec["id"]
        flds   = rec.get("fields", {})
        username = flds.get("📸 Username")
        logging.info(f"Processing competitor '{username}' (record {rec_id})")
        if not username:
            logging.warning(f"Skipping competitor with no username (record {rec_id})")
            continue

        try:
            resp = requests.get(endpoint, headers=rapid_headers, params={"username_or_id": username})
            resp.raise_for_status()
            data = resp.json().get("data")
            if not isinstance(data, dict):
                raise ValueError("no data object")

            full_name       = data.get("full_name", "")
            pic_url         = data["hd_profile_pic_url_info"]["url"]
            followers       = data.get("follower_count", 0)
            followings      = data.get("following_count", 0)
            posts           = data.get("media_count", 0)
            bio             = data.get("biography", "")
            bio_link        = data.get("external_url", "")

            update_fields = {
                "🗒️ Name": full_name,
                "🖼️ PFP": [{"url": pic_url}],
                "🔢 Followers": followers,
                "🚹 Followings": followings,
                "#️⃣ Number of posts": posts,
                "📓 Bio": bio,
                "📓 Bio link": bio_link
            }
            url     = f"{base_url}/{quote('🎯 Competitor Accounts')}"
            payload = {"records": [{"id": rec_id, "fields": update_fields}]}
            r = requests.patch(url, json=payload, headers=airtable_headers)
            r.raise_for_status()
            logging.info(f"Updated competitor '{username}'")

        except (requests.RequestException, KeyError, ValueError) as e:
            logging.error(f"Skipping competitor '{username}': {e}")
            continue

    logging.info("Completed update_competitor_accounts")

def scrape_competitor_reels(api_key, base_id, rapidapi_key):
    """
    For each record in "🎯 Competitor Accounts", fetch all reels via RapidAPI (paginated),
    then insert each reel from the past 30 days into "🎥 Competitor Reels",
    skipping any duplicates or malformed JSON.
    """
    import requests
    import logging
    from urllib.parse import quote
    import datetime

    logging.info("Starting scrape_competitor_reels")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.airtable.com/v0/{base_id}"
    reels_url = "https://real-time-instagram-scraper-api1.p.rapidapi.com/v1/user_reels"
    rapid_headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "real-time-instagram-scraper-api1.p.rapidapi.com"
    }

    # 1) load existing reel IDs
    existing = fetch_table_records(api_key, base_id, "🎥 Competitor Reels")
    existing_ids = {
        rec["fields"].get("🤖 Reel ID")
        for rec in existing
        if rec.get("fields", {}).get("🤖 Reel ID")
    }
    logging.info(f"Loaded {len(existing_ids)} existing competitor reel IDs")

    # 2) fetch competitor accounts
    accounts = fetch_table_records(api_key, base_id, "🎯 Competitor Accounts")
    logging.info(f"Processing {len(accounts)} competitor accounts")
    cutoff = datetime.datetime.now() - datetime.timedelta(days=30)

    for acc in accounts:
        acct_id  = acc["id"]
        flds     = acc.get("fields", {})
        username = flds.get("📸 Username")
        snapshot = flds.get("🔢 Followers", 0)
        if not username:
            logging.warning(f"Skipping competitor with missing username (record {acct_id})")
            continue

        logging.info(f"Scraping reels for competitor @{username}")
        max_id = None

        try:
            while True:
                params = {"username_or_id": username}
                if max_id:
                    params["max_id"] = max_id

                resp = requests.get(reels_url, headers=rapid_headers, params=params, timeout=15)
                resp.raise_for_status()
                body = resp.json()
                data = body.get("data")
                if not isinstance(data, dict):
                    logging.error(f"Malformed JSON for @{username}: {body}")
                    raise ValueError("malformed data")

                items = data.get("items", [])
                logging.info(f"Fetched {len(items)} items for @{username} (max_id={max_id})")

                for item in items:
                    try:
                        m = item["media"]
                        reel_id = m.get("code")
                        if not reel_id:
                            logging.debug("Skipping item with no 'code'")
                            continue
                        if reel_id in existing_ids:
                            logging.debug(f"Skipping duplicate reel {reel_id}")
                            continue

                        posted = datetime.datetime.fromtimestamp(m["taken_at"])
                        if posted < cutoff:
                            logging.debug(f"Skipping old reel {reel_id} posted on {posted}")
                            continue

                        cap = m.get("caption") or {}
                        caption_text = cap.get("text", "")

                        vid = max(
                            m.get("video_versions", []),
                            key=lambda v: v.get("height", 0),
                            default={}
                        )
                        download_link = vid.get("url", "")

                        record = {
                            "🔒 Account": [acct_id],
                            "🔢 Followers Snapshot": snapshot,
                            "📒 Caption": caption_text,
                            "💬 Comment count": m.get("comment_count", 0),
                            "👀 Views": m.get("play_count", 0),
                            "👍 Like count": m.get("like_count", 0),
                            "🗓️ Date of posting": posted.date().isoformat(),
                            "🤖 Reel ID": reel_id,
                            "⬇️ Download link": download_link
                        }

                        url = f"{base_url}/{quote('🎥 Competitor Reels')}"
                        payload = {"records": [{"fields": record}]}
                        r = requests.post(url, json=payload, headers=headers, timeout=15)
                        r.raise_for_status()
                        existing_ids.add(reel_id)
                        logging.info(f"Inserted competitor reel {reel_id}")

                    except (KeyError, TypeError, ValueError) as e:
                        logging.warning(f"Skipping malformed competitor reel for @{username}: {e}")
                        continue

                # 3) pagination: get max_id from paging_info
                paging = data.get("paging_info", {})
                new_max = paging.get("max_id")
                more_available = paging.get("more_available", False)
                logging.info(f"Paging info for @{username}: more_available={more_available}, next_max_id={new_max}")

                if not more_available or new_max == max_id:
                    logging.info("No more pages or max_id unchanged; breaking pagination")
                    break
                max_id = new_max
                logging.info(f"Next page max_id={max_id}")

        except requests.exceptions.Timeout as e:
            logging.error(f"Timeout fetching reels for '{username}': {e}")
            continue
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error for '{username}': {e}; response={e.response.text}")
            continue
        except ValueError as e:
            logging.error(f"Value error for '{username}': {e}")
            continue

    logging.info("Completed scrape_competitor_reels")



def update_swarm_post_count(api_key, base_id, rapidapi_key):
    """
    For each record in "⚡ SWARM", fetches Instagram user_info via RapidAPI
    and updates the "Post count" field in Airtable with the 'media_count' value.
    """
    logging.info("Starting update_swarm_post_count")
    from urllib.parse import quote

    airtable_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.airtable.com/v0/{base_id}"
    table_name = "⚡ SWARM"
    endpoint = "https://real-time-instagram-scraper-api1.p.rapidapi.com/v1/user_info"
    rapid_headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "real-time-instagram-scraper-api1.p.rapidapi.com"
    }

    records = fetch_table_records(api_key, base_id, table_name)
    logging.info(f"Fetched {len(records)} SWARM records")

    for rec in records:
        rec_id = rec["id"]
        flds   = rec.get("fields", {})
        username = flds.get("Username")
        logging.info(f"Processing SWARM account '{username}'")

        if not username:
            logging.warning("Skipping due to missing username")
            continue

        try:
            resp = requests.get(endpoint, headers=rapid_headers, params={"username_or_id": username})
            resp.raise_for_status()
            data = resp.json().get("data")
            if not isinstance(data, dict):
                raise ValueError("malformed 'data' object")

            post_count = data.get("media_count", 0)
            logging.debug(f"{username}: media_count={post_count}")

            update_fields = {"Post count": post_count}
            url = f"{base_url}/{quote(table_name)}"
            payload = {"records": [{"id": rec_id, "fields": update_fields}]}
            patch = requests.patch(url, json=payload, headers=airtable_headers)
            patch.raise_for_status()
            logging.info(f"Updated Post count for '{username}'")

        except (requests.RequestException, KeyError, ValueError) as e:
            logging.error(f"Skipping '{username}': {e}")
            continue

    logging.info("Completed update_swarm_post_count")

def update_swarm_account_status(api_key, base_id, posts_ready, days_ready):
    """
    In “⚡ SWARM”, sets Status="READY" for any record where
    Post count ≥ posts_ready AND Day ≥ days_ready.
    """
    logging.info("Starting update_swarm_account_status")
    from urllib.parse import quote

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.airtable.com/v0/{base_id}"
    table = "⚡ SWARM"

    records = fetch_table_records(api_key, base_id, table)
    logging.info(f"Fetched {len(records)} SWARM records for status update")

    to_update = []
    for rec in records:
        rid = rec["id"]
        f = rec.get("fields", {})
        if f.get("Status") == "READY":
            continue
        if f.get("Post count", 0) >= posts_ready and f.get("Day", 0) >= days_ready:
            to_update.append({"id": rid, "fields": {"Status": "READY"}})
            logging.info(f"Marking record {rid} READY")

    if to_update:
        url = f"{base_url}/{quote(table)}"
        payload = {"records": to_update}
        r = requests.patch(url, json=payload, headers=headers)
        r.raise_for_status()
        logging.info(f"Updated {len(to_update)} SWARM records to READY")
    else:
        logging.info("No SWARM records needed updating")

    logging.info("Completed update_swarm_account_status")

def update_reel_account_growth_mode(api_key, base_id, posts_ready, days_ready):
    """
    In “🤩 Agency Accounts”, sets Account Status="Growth mode" for any record where
    Posts ≥ posts_ready AND Day ≥ days_ready.
    """
    logging.info("Starting update_reel_account_growth_mode")
    from urllib.parse import quote

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    base_url = f"https://api.airtable.com/v0/{base_id}"
    table = "🤩 Agency Accounts"

    records = fetch_table_records(api_key, base_id, table)
    logging.info(f"Fetched {len(records)} agency accounts for growth mode check")

    to_update = []
    for rec in records:
        rid = rec["id"]
        f = rec.get("fields", {})
        current = f.get("Account Status")
        posts = f.get("Posts", 0)
        day   = f.get("Day", 0)
        logging.debug(f"Account {rid}: Status={current}, Posts={posts}, Day={day}")

        if current == "Growth mode":
            continue
        if posts >= posts_ready and day >= days_ready:
            to_update.append(rid)
            logging.info(f"Scheduling Growth mode for {rid}")

    if not to_update:
        logging.info("No agency accounts to update for growth mode")
        return

    for rid in to_update:
        update_fields = {"Account Status": "Growth mode"}
        url = f"{base_url}/{quote(table)}"
        payload = {"records": [{"id": rid, "fields": update_fields}]}
        try:
            r = requests.patch(url, json=payload, headers=headers)
            r.raise_for_status()
            logging.info(f"Set Growth mode for record {rid}")
        except requests.exceptions.HTTPError as e:
            logging.error(f"Failed updating record {rid}: {e}; response: {r.text}")

    logging.info("Completed update_reel_account_growth_mode")

def notify_viral_reels(api_key, base_id):
    """
    Scans “🎥 Agency Reels” for any reel whose 🎯 Virality score ×100 ≥ threshold
    and whose 🌘 Virality notification checkbox is False.
    Sends a Telegram video via the HTTP Bot API, then ticks the checkbox.
    Handles chat migration automatically.
    """
    logging.info("Starting notify_viral_reels")
    from urllib.parse import quote
    from io import BytesIO

    settings = records_to_dataframe(
        fetch_table_records(api_key, base_id, "🔑 Automation settings")
    )
    threshold = float(settings.loc[settings["Name"]=="VIRALITY_PERCENTAGE_TO_AVG", "Value"].iat[0])
    bot_token   = settings.loc[settings["Name"]=="TELEGRAM_BOT_API_KEY", "Value"].iat[0]
    chat_id_raw = settings.loc[settings["Name"]=="TELEGRAM_GROUP_ID",    "Value"].iat[0]
    try:
        chat_id = int(chat_id_raw)
    except ValueError:
        chat_id = chat_id_raw

    send_msg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    def get_valid_chat_id(cid):
        r = requests.post(send_msg_url, data={"chat_id": cid, "text": "🔔 Bot online"})
        js = r.json()
        if js.get("ok"):
            return cid
        params = js.get("parameters") or {}
        mig = params.get("migrate_to_chat_id")
        if mig:
            r2 = requests.post(send_msg_url, data={"chat_id": mig, "text": "🔔 Bot online"})
            r2.raise_for_status()
            return mig
        logging.error(f"sendMessage failed for {cid}: {js}")
        return None

    valid_chat = get_valid_chat_id(chat_id)
    if not valid_chat:
        logging.error("Cannot send test message; aborting notify_viral_reels")
        return
    chat_id = valid_chat

    accounts = fetch_table_records(api_key, base_id, "🤩 Agency Accounts")
    id2user = {a["id"]: a["fields"].get("📸 Username","") for a in accounts}
    reels   = fetch_table_records(api_key, base_id, "🎥 Agency Reels")
    logging.info(f"Evaluating {len(reels)} reels for viral notifications")

    for idx, reel in enumerate(reels, start=1):
        rid     = reel["id"]
        flds    = reel.get("fields", {})
        vir_pct = flds.get("Virality score", 0) * 100
        notified= flds.get("🌘 Virality notification", False)
        logging.info(f"[{idx}/{len(reels)}] Reel {rid}: virality={vir_pct:.2f}%, notified={notified}")

        if notified or vir_pct < threshold:
            continue

        code    = flds.get("🤖 Reel ID","")
        dl_link = flds.get("⬇️ Download link","")
        views   = flds.get("👀 Views",0)
        likes   = flds.get("👍 Like count",0)
        comments= flds.get("💬 Comment count",0)
        caption = flds.get("📒 Caption","")
        acct_id = (flds.get("🔒 Account") or [None])[0]
        username= id2user.get(acct_id,"")

        video_bytes = None
        try:
            r = requests.get(dl_link); r.raise_for_status()
            video_bytes = BytesIO(r.content)
            video_bytes.name = f"{code}.mp4"
            video_bytes.seek(0)
            logging.info(f"Downloaded video for reel {rid}")
        except Exception as e:
            logging.warning(f"Video download failed for {rid}: {e}")

        base_text = (
            f"📈 @{username} just had a viral reel! (+{vir_pct:.2f}% over avg)\n\n"
            f"👀 Views: {views}\n"
            f"👍 Likes: {likes}\n"
            f"💬 Comments: {comments}\n\n"
            f"🔗 https://www.instagram.com/reel/{code}\n\n"
            f"💬 {caption}"
        )

        if video_bytes:
            send_vid_url = f"https://api.telegram.org/bot{bot_token}/sendVideo"
            data = {"chat_id": chat_id, "caption": base_text}
            files = {"video": (video_bytes.name, video_bytes, "video/mp4")}
            try:
                rv = requests.post(send_vid_url, data=data, files=files)
                rv.raise_for_status()
                logging.info(f"sendVideo OK for reel {rid}")
            except Exception as e:
                logging.error(f"sendVideo failed for reel {rid}: {e}")
                video_bytes = None

        if not video_bytes:
            text_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            text = base_text + "\n\n⚠️ Video link expired."
            try:
                rt = requests.post(text_url, data={"chat_id": chat_id, "text": text})
                rt.raise_for_status()
                logging.info(f"sendMessage OK (no video) for reel {rid}")
            except Exception as e:
                logging.error(f"sendMessage failed for reel {rid}: {e}")

        try:
            patch_url = f"https://api.airtable.com/v0/{base_id}/{requests.utils.quote('🎥 Agency Reels')}"
            payload = {"records":[{"id":rid,"fields":{"🌘 Virality notification":True}}]}
            rp = requests.patch(patch_url, json=payload, headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            })
            rp.raise_for_status()
            logging.info(f"Marked notification for reel {rid}")
        except Exception as e:
            logging.error(f"Airtable update failed for reel {rid}: {e}")

    logging.info("Completed notify_viral_reels")

def main():
    logging.info("=== Starting main ===")

    # 1) Load credentials
    cfg = load_config()
    api_key = cfg["api_key"]
    base_id = cfg["base_id"]

    # 2) Fetch settings table
    try:
        logging.info("Fetching '🔑 Automation settings'")
        recs = fetch_table_records(api_key, base_id, "🔑 Automation settings")
        settings_table = records_to_dataframe(recs)
    except Exception as e:
        logging.exception("Failed to fetch or parse Automation settings, exiting")
        return

    # grab rapidapi key
    try:
        rapidapi_key = settings_table.loc[
            settings_table["Name"]=="RAPIDAPI_KEY", "Value"
        ].iat[0]
    except Exception as e:
        logging.exception("Failed to read RAPIDAPI_KEY from settings, exiting")
        return

    # define each step as (name, func)
    steps = [
        ("Fetch agency accounts DF",    lambda: fetch_agency_accounts(api_key, base_id)),
        ("Update agency account stats", lambda: update_agency_account_stats(api_key, base_id, rapidapi_key)),
        ("Scrape agency reels",         lambda: scrape_agency_reels(api_key, base_id, rapidapi_key)),
        ("Calc & update account stats", lambda: calculate_and_update_account_stats(api_key, base_id)),
        ("Update competitor accounts",   lambda: update_competitor_accounts(api_key, base_id, rapidapi_key)),
        ("Scrape competitor reels",      lambda: scrape_competitor_reels(api_key, base_id, rapidapi_key)),
        ("Update SWARM post count",     lambda: update_swarm_post_count(api_key, base_id, rapidapi_key)),
        ("Update SWARM status",         lambda: update_swarm_account_status(
                                            api_key, base_id,
                                            int(settings_table.loc[
                                                settings_table["Name"]=="WHEN_SWARM_ACCOUNT_READY_POSTS","Value"
                                            ].iat[0]),
                                            int(settings_table.loc[
                                                settings_table["Name"]=="WHEN_SWARM_ACCOUNT_READY_DAYS","Value"
                                            ].iat[0])
                                         )),
        ("Update growth mode",          lambda: update_reel_account_growth_mode(
                                            api_key, base_id,
                                            int(settings_table.loc[
                                                settings_table["Name"]=="WHEN_REEL_ACCOUNT_READY_POSTS","Value"
                                            ].iat[0]),
                                            int(settings_table.loc[
                                                settings_table["Name"]=="WHEN_REEL_ACCOUNT_READY_DAY","Value"
                                            ].iat[0])
                                         )),
        ("Notify viral reels",          lambda: notify_viral_reels(api_key, base_id)),
    ]

    for name, func in steps:
        logging.info(f"--- Starting: {name} ---")
        try:
            func()
            logging.info(f"--- Completed: {name} ---")
        except Exception:
            logging.exception(f"Error during step: {name}  (continuing to next)")

    logging.info("=== Finished main ===")


if __name__ == "__main__":
    main()
