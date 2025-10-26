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
    # Directly use the YouTube API key
    return build("youtube", "v3", developerKey="<Youtube API Key>")


def upload_to_gcs(bucket_name, destination_blob, local_file_path):
    """Upload a local file to Google Cloud Storage as NDJSON."""
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


def main():
    """Main function to fetch YouTube comments and save/upload."""
    youtube = initialize_youtube_client()

    print("[INFO] Starting YouTube comment ingestion...")
    response = {"items": []}

    # Fetch latest comments from the specified YouTube channel
    try:
        if youtube:
            request = youtube.commentThreads().list(
                part="snippet",
                allThreadsRelatedToChannelId="UCjGVPGkLwMdu8LuR637tcNA",
                maxResults=100
            )
            response = request.execute()
            print(f"[INFO] Retrieved {len(response.get('items', []))} comments from YouTube API.")
        else:
            print("[INFO] Mock mode - no API call performed.")
    except Exception as e:
        print(f"[ERROR] Failed to fetch YouTube comments: {e}")
        return

    # Extract comments into a list of dictionaries
    output_comments = []
    for item in response.get("items", []):
        try:
            comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            comment_id = item["snippet"]["topLevelComment"]["id"]
            output_comments.append({
                "id": comment_id,
                "comment": comment_text
            })
        except KeyError as e:
            print(f"[ERROR] Missing key while processing comment: {e}")

    # Save data to a local NDJSON file
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

    # Upload the file to GCS
    gcs_bucket = "yt-raw-comments-dna-poc-training"
    gcs_path = "youtube_comments/output_spice_eats.json"

    upload_to_gcs(gcs_bucket, gcs_path, local_file)


if __name__ == "__main__":
    main()
