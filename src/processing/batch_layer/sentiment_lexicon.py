from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

nltk.download('vader_lexicon', quiet=True)

class SentimentAnalyzer:
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()
        
        crypto_lexicon = {
            'rekt': -3.5,
            'rugpull': -4.0,
            'rug': -3.5,
            'scam': -3.5,
            'dump': -2.5,
            'bearish': -2.5,
            'fud': -2.0,
            'ngmi': -2.0,
            'red': -1.5,
            'moon': 3.5,
            'lambo': 3.0,
            'bullish': 2.5,
            'pump': 2.5,
            'hodl': 2.0,
            'wagmi': 2.0,
            'gem': 2.0,
            'ath': 2.0,
            'ape': 1.5,
            'green': 1.5,
        }
        self.sia.lexicon.update(crypto_lexicon)

    def get_score(self, text):
        return self.sia.polarity_scores(text or '')['compound']