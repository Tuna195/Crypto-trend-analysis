from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import re

nltk.download('vader_lexicon', quiet=True)

class SentimentAnalyzer:
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()
        crypto_lexicon = {
            'bullish': 3.0,
            'moon': 3.5,
            'lambo': 3.0,
            'hodl': 2.0,
            'wagmi': 2.5,
            'ath': 3.0,          
            'breakout': 2.5,     
            'accumulation': 1.5, 
            'accumulate': 1.5,
            'momentum': 1.8,    
            'expansion': 1.5,    
            'constructive': 1.5, 
            'recovery': 2.0,    
            'outperformed': 2.0, 
            'outperform': 2.0,
            'gain': 1.5,
            'gained': 1.5,
            'rose': 1.5,       
            'flywheel': 1.8,    
            'breakthrough': 2.5, 
            'breakthroughs': 2.5,
            'support': 1.5,      
            'pump': 2.0,
            'gem': 2.0,
            'green': 1.5,
            'bearish': -3.0,
            'rekt': -3.5,        
            'rugpull': -4.0,     
            'rug': -3.5,
            'scam': -3.5,
            'dump': -2.5,
            'fud': -2.0,        
            'ngmi': -2.5,        
            'crisis': -2.5,      
            'worsening': -2.5,  
            'weak': -1.5,        
            'declining': -1.5,   
            'pressure': -1.0,    
            'liquidate': -2.0,  
            'liquidated': -2.5,
            'liquidation': -2.0,
            'short': -1.5,       
            'red': -1.5,
            'crash': -3.0,
            'dead': -2.5,
        }
        self.sia.lexicon.update(crypto_lexicon)
        self.url_pattern = re.compile(r'https?://\S+|www\.\S+')
        self.cashtag_pattern = re.compile(r'\$([A-Za-z]+)')
        self.hashtag_pattern = re.compile(r'#([A-Za-z0-9_]+)')

    def preprocess_text(self, text: str) -> str:
        if not text:
            return ""
        
        cleaned = self.url_pattern.sub("", text)
        
        cleaned = self.cashtag_pattern.sub(r"\1", cleaned)
        
        cleaned = self.hashtag_pattern.sub(r"\1", cleaned)
        
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned

    def get_score(self, text):
        cleaned_text = self.preprocess_text(text)
        return self.sia.polarity_scores(cleaned_text)['compound']