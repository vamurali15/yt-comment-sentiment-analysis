import time
from google.cloud import language_v1
from google.cloud import bigquery
from google.api_core.exceptions import ResourceExhausted

# Constants
TOTAL_TO_PROCESS = 5500
BATCH_SIZE = 100  # Number of comments per batch
SLEEP_BETWEEN_REQUESTS = 0.1  # 0.1 sec/request = 10 requests/sec = 600/min

def get_comments_batch(bq_client, offset, batch_size):
    query = f"""
    SELECT comment_id, comment_text
    FROM `dna-poc-training.youtube_comments.comments1`
    ORDER BY comment_id
    LIMIT {batch_size} OFFSET {offset}
    """
    return bq_client.query(query).result()

def analyze_sentiment_batch(client, comments):
    rows_to_insert = []
    for row in comments:
        doc = language_v1.Document(
            content=row.comment_text,
            type_=language_v1.Document.Type.PLAIN_TEXT,
        )
        # Robust error handling
        while True:
            try:
                sentiment = client.analyze_sentiment(request={"document": doc}).document_sentiment
                break
            except ResourceExhausted:
                print("Quota exceeded, waiting 60 seconds before retrying...")
                time.sleep(60)
        rows_to_insert.append({
            "comment_id": row.comment_id,
            "comment_text": row.comment_text,
            "sentiment_score": sentiment.score,
            "sentiment_magnitude": sentiment.magnitude,
        })
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return rows_to_insert

def main():
    client = language_v1.LanguageServiceClient()
    bq_client = bigquery.Client()
    table_id = "dna-poc-training.youtube_comments.sentiment"

    for offset in range(0, TOTAL_TO_PROCESS, BATCH_SIZE):
        comments = get_comments_batch(bq_client, offset, BATCH_SIZE)
        rows_to_insert = analyze_sentiment_batch(client, comments)
        bq_client.insert_rows_json(table_id, rows_to_insert)
        print(f"Processed batch: {offset}–{offset + len(rows_to_insert)}")

if __name__ == "__main__":
    main()
