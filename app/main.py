import json

try:
    from googleapiclient.discovery import build
    from google.cloud import storage
except ImportError:
    build = None
    storage = None


def initialize_youtube_client():
    """Initialize YouTube API client."""
    if build is None:
        print("[WARNING] googleapiclient not installed; skipping actual API call.")
        return None
    # Use the YouTube API key directly
    return build("youtube", "v3", developerKey="AIzaSyAMJHhB2C9EQq-F2Cm-_ULWEWny7n40gSM")


def upload_to_gcs(bucket_name, destination_blob, local_file_path):
    """Upload a local file to Google Cloud Storage."""
    if storage is None:
        print("[WARNING] google-cloud-storage not installed; skipping GCS upload.")
        return

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_file_path)
        print(f"[SUCCESS] Uploaded to gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        print(f"[ERROR] GCS upload failed: {e}")


def fetch_youtube_comments(youtube, channel_id, limit=1000):
    """Fetch up to `limit` YouTube comments using pagination."""
    all_comments = []
    next_page_token = None

    while True:
        request = youtube.commentThreads().list(
            part="snippet",
            allThreadsRelatedToChannelId=channel_id,
            maxResults=100,
            pageToken=next_page_token
        )
        response = request.execute()
        items = response.get("items", [])
        all_comments.extend(items)
        print(f"[INFO] Retrieved {len(all_comments)} comments so far...")

        next_page_token = response.get("nextPageToken")
        if not next_page_token or len(all_comments) >= limit:
            break

    print(f"[INFO] Total comments fetched: {len(all_comments)}")
    return all_comments


def main():
    """Main function: fetch YouTube comments and upload to GCS."""
    youtube = initialize_youtube_client()
    print("[INFO] Starting YouTube comment ingestion...")

    response_items = []
    try:
        if youtube:
            response_items = fetch_youtube_comments(
                youtube,
                channel_id="UCjGVPGkLwMdu8LuR637tcNA",
                limit=1000
            )
        else:
            print("[INFO] Mock mode - no API call performed.")
    except Exception as e:
        print(f"[ERROR] Failed to fetch YouTube comments: {e}")
        return

    # Process comments into simple dictionaries
    output_comments = []
    for item in response_items:
        try:
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            output_comments.append({
                "id": item["snippet"]["topLevelComment"]["id"],
                "author": snippet.get("authorDisplayName", ""),
                "published_at": snippet.get("publishedAt", ""),
                "comment": snippet.get("textDisplay", "")
            })
        except KeyError as e:
            print(f"[ERROR] Missing key while processing comment: {e}")

    # Save locally as NDJSON
    local_file = "output_spice_eats.json"
    try:
        with open(local_file, "w", encoding="utf-8") as out_f:
            for comment in output_comments:
                json.dump(comment, out_f, ensure_ascii=False)
                out_f.write("\n")
        print(f"[SUCCESS] Saved {len(output_comments)} comments to {local_file}")
    except Exception as e:
        print(f"[ERROR] Failed to save comments locally: {e}")
        return

    # Upload to GCS
    gcs_bucket = "yt-raw-comments-dna-poc-training"
    gcs_path = "youtube_comments/output_spice_eats.json"
    upload_to_gcs(gcs_bucket, gcs_path, local_file)


if __name__ == "__main__":
    main()
