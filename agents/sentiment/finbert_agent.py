from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import pandas as pd
from typing import List
import os
import duckdb

class FinbertSentimentAgent:
    def __init__(self, model_name="ProsusAI/finbert"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.sentiment_labels = ["negative", "neutral", "positive"]

    def run_sentiment(self, df_news: pd.DataFrame) -> pd.DataFrame:
        """Applies FinBERT sentiment analysis to news headlines and returns sentiment scores and confidence."""
        if df_news.empty:
            return pd.DataFrame(columns=['ts', 'symbol', 'news_sent', 'news_conf'])

        texts = df_news['title'].tolist() # Using title for sentiment as per Module-Data-Engineering.md
        
        # Tokenize and predict in batches to handle large dataframes efficiently
        batch_size = 32
        all_sentiment_scores = []
        all_confidence_scores = []

        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i+batch_size]
            inputs = self.tokenizer(batch_texts, padding=True, truncation=True, return_tensors='pt')
            
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            probabilities = torch.softmax(outputs.logits, dim=1)
            
            # Map probabilities to sentiment score [-1, 1] and confidence [0, 1]
            # Assuming sentiment_labels are ordered: negative, neutral, positive
            for probs in probabilities:
                neg_score = probs[self.sentiment_labels.index("negative")]
                neu_score = probs[self.sentiment_labels.index("neutral")]
                pos_score = probs[self.sentiment_labels.index("positive")]
                
                sentiment_score = pos_score.item() - neg_score.item()
                # Confidence can be thought of as the max probability or 1 - neutral_probability
                confidence = (pos_score + neg_score).item() # Confidence in being non-neutral
                
                all_sentiment_scores.append(sentiment_score)
                all_confidence_scores.append(confidence)

        df_sentiment = df_news.copy()
        df_sentiment['news_sent'] = all_sentiment_scores
        df_sentiment['news_conf'] = all_confidence_scores

        # Select and return required columns
        return df_sentiment[['ts', 'symbol', 'news_sent', 'news_conf']]

if __name__ == "__main__":
    # Example Usage:
    # This assumes normalized news data is available in data/lake/news_norm/news_norm.parquet
    
    # Create a dummy news_norm.parquet for testing if it doesn't exist
    news_norm_path = 'data/lake/news_norm/news_norm.parquet'
    if not os.path.exists(news_norm_path):
        print(f"Dummy news_norm.parquet not found at {news_norm_path}. Creating for testing.")
        dummy_data = {
            'ts': ['2023-01-01T10:00:00Z', '2023-01-01T11:00:00Z', '2023-01-01T12:00:00Z'],
            'symbol': ['AAPL', 'MSFT', 'AAPL'],
            'source': ['Reuters', 'Bloomberg', 'AP'],
            'title': ['Apple stock rises on strong iPhone sales', 'Microsoft announces new cloud initiatives', 'Apple faces antitrust scrutiny'],
            'text': ['...', '...', '...'],
            'url': ['http://example.com/1', 'http://example.com/2', 'http://example.com/3']
        }
        pd.DataFrame(dummy_data).to_parquet(news_norm_path, index=False)

    # Load normalized news data
    conn = duckdb.connect(database='./data/trading.duckdb', read_only=False)
    conn.execute("CREATE OR REPLACE VIEW news_norm AS SELECT * FROM parquet_scan('data/lake/news_norm/*.parquet');")
    df_news_norm = conn.execute("SELECT * FROM news_norm").fetchdf()
    conn.close()

    agent = FinbertSentimentAgent()
    df_sentiment_results = agent.run_sentiment(df_news_norm)
    print("Sentiment Analysis Results:")
    print(df_sentiment_results)
