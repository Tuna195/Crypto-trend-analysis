"""Bot and spam detection filter for batch layer.

Detects suspicious accounts and content patterns using:
- Account metrics (followers, account age via timestamp patterns)
- Content patterns (emoji abuse, repeated hashtags, mention spam, URL flooding)
- Behavioral patterns (repetition, excessive links, suspicious keywords)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# Spam/bot detection patterns
SPAM_KEYWORDS = {
    "follow back", "follow me", "followers", "likes", "retweets",
    "check bio", "link in bio", "dm me", "subscribe", "click here",
    "giveaway", "airdrop", "free tokens", "moon coin", "pump",
    "buy low sell high", "guaranteed gains", "invest now",
    "forex", "crypto signal", "trading signal", "get rich",
    "limited time", "hurry up", "act now", "don't miss",
}

# Common spam emoji patterns (overuse of certain emojis indicates spam)
SPAM_EMOJIS = {"🚀", "💎", "🔥", "💰", "📈", "🌙", "⚡", "💯", "✅", "👏"}

# Suspicious/bot keywords
BOT_KEYWORDS = {
    "automated", "bot", "ai trading", "robot", "algorithm",
    "auto reply", "autoposting", "scheduled", "marketing bot",
    "viral", "viral marketing", "engagement pod",
}

# Crypto scam/rug pull patterns
SCAM_PATTERNS = {
    "presale", "private sale", "whitelist", "burn", "stake rewards",
    "passive income", "work from home", "easy money",
}

# URL pattern (spam often has many URLs)
URL_PATTERN = re.compile(r'https?://|bit\.ly|t\.co|short\.link')

# Mention pattern
MENTION_PATTERN = re.compile(r'@\w+')

# Hashtag pattern
HASHTAG_PATTERN = re.compile(r'#\w+')

# Cashtag/symbol pattern
CASHTAG_PATTERN = re.compile(r'\$[A-Z]{1,10}')


@dataclass
class SpamScore:
    """Spam detection result."""
    total_score: float  # 0.0 to 1.0
    is_spam: bool
    is_bot: bool
    reasons: list[str]
    content_patterns: dict[str, float]


class BotSpamFilter:
    """Detects bot and spam tweets using pattern matching and heuristics."""

    def __init__(
        self,
        spam_threshold: float = 0.3,
        bot_threshold: float = 0.3,
        min_content_length: int = 5,
    ):
        """
        Args:
            spam_threshold: Score above this triggers spam flag (0.0-1.0)
            bot_threshold: Score above this triggers bot flag (0.0-1.0)
            min_content_length: Minimum tweet length (chars)
        """
        self.spam_threshold = spam_threshold
        self.bot_threshold = bot_threshold
        self.min_content_length = min_content_length

    def detect_spam(
        self,
        content: str,
        username: Optional[str] = None,
        engagement_score: int = 0,
        author_weight: float = 1.0,
    ) -> SpamScore:
        """
        Detect spam and bot patterns in a tweet.

        Args:
            content: Tweet text content
            username: Tweet author username
            engagement_score: Sum of likes + retweets + replies
            author_weight: Weight score (5.0 = whale, 1.0 = normal)

        Returns:
            SpamScore with detection results and reasoning
        """
        reasons = []
        patterns = {}

        # Normalize content
        content_lower = content.lower()
        content_len = len(content)

        # --- Content Length Check ---
        if content_len < self.min_content_length:
            reasons.append("content_too_short")
            patterns["short_content"] = 1.0
        else:
            patterns["short_content"] = 0.0

        # --- Emoji Abuse Check ---
        emoji_score = self._check_emoji_abuse(content)
        patterns["emoji_abuse"] = emoji_score
        if emoji_score > 0.6:
            reasons.append(f"emoji_abuse(score={emoji_score:.2f})")

        # --- URL/Link Flooding Check ---
        url_score = self._check_url_flooding(content)
        patterns["url_flooding"] = url_score
        if url_score > 0.5:
            reasons.append(f"url_flooding(count={int(url_score * 10)})")

        # --- Mention/Hashtag/Cashtag Spam Check ---
        mention_score = self._check_mention_spam(content)
        patterns["mention_spam"] = mention_score
        if mention_score > 0.6:
            reasons.append(f"mention_spam(score={mention_score:.2f})")

        hashtag_score = self._check_hashtag_spam(content)
        patterns["hashtag_spam"] = hashtag_score
        if hashtag_score > 0.6:
            reasons.append(f"hashtag_spam(score={hashtag_score:.2f})")

        # --- Spam Keywords Check ---
        spam_keyword_score = self._check_spam_keywords(content_lower)
        patterns["spam_keywords"] = spam_keyword_score
        if spam_keyword_score > 0.0:
            reasons.append(f"spam_keywords(score={spam_keyword_score:.2f})")

        # --- Scam Pattern Check ---
        scam_score = self._check_scam_patterns(content_lower)
        patterns["scam_patterns"] = scam_score
        if scam_score > 0.0:
            reasons.append(f"scam_patterns(score={scam_score:.2f})")

        # --- Bot Keywords Check ---
        bot_keyword_score = self._check_bot_keywords(content_lower)
        patterns["bot_keywords"] = bot_keyword_score
        if bot_keyword_score > 0.0:
            reasons.append(f"bot_keywords(score={bot_keyword_score:.2f})")

        # --- Username Pattern Check ---
        username_score = self._check_username_pattern(username or "")
        patterns["username_pattern"] = username_score
        if username_score > 0.5:
            reasons.append(f"username_pattern(score={username_score:.2f})")

        # --- Engagement Anomaly Check ---
        # New/low-weight accounts with high engagement = suspicious
        engagement_score_norm = self._check_engagement_anomaly(
            engagement_score, author_weight, content_len
        )
        patterns["engagement_anomaly"] = engagement_score_norm
        if engagement_score_norm > 0.6:
            reasons.append(f"engagement_anomaly(score={engagement_score_norm:.2f})")

        # --- Content Pattern Check (bot-like content) ---
        content_pattern_score = self._check_content_patterns(content_lower)
        patterns["content_patterns"] = content_pattern_score
        if content_pattern_score > 0.0:
            reasons.append(f"content_patterns(score={content_pattern_score:.2f})")

        # --- Calculate Overall Scores ---
        # Weight the patterns
        weights = {
            "emoji_abuse": 0.10,
            "url_flooding": 0.12,
            "mention_spam": 0.10,
            "hashtag_spam": 0.08,
            "spam_keywords": 0.35,
            "scam_patterns": 0.18,
            "bot_keywords": 0.20,  # Increased from 0.15
            "username_pattern": 0.12,  # Increased from 0.08
            "engagement_anomaly": 0.06,
            "content_patterns": 0.15,  # New pattern
            "short_content": 0.00,
        }

        spam_score = sum(patterns.get(k, 0.0) * weights.get(k, 0.0) for k in weights)
        spam_score = min(1.0, spam_score)  # Clamp to [0, 1]

        # Bot detection (focus on keyword/pattern indicators)
        bot_score = (
            bot_keyword_score * 0.35  # Increased from 0.4
            + username_score * 0.35    # Increased from 0.3
            + content_pattern_score * 0.30  # New component
        )
        bot_score = min(1.0, bot_score)

        is_spam = spam_score >= self.spam_threshold
        is_bot = bot_score >= self.bot_threshold

        return SpamScore(
            total_score=spam_score,
            is_spam=is_spam,
            is_bot=is_bot,
            reasons=reasons,
            content_patterns=patterns,
        )

    def _check_emoji_abuse(self, content: str) -> float:
        """Check for excessive or suspicious emoji use."""
        # Count emojis (rough estimate)
        emoji_count = len(re.findall(r'[\U0001F300-\U0001F9FF]', content))
        
        if emoji_count == 0:
            return 0.0
        
        # Check for spam emojis specifically
        spam_emoji_count = sum(
            1 for emoji in SPAM_EMOJIS if emoji in content
        )
        
        # Ratio of spam emoji to total emoji
        spam_emoji_ratio = spam_emoji_count / emoji_count if emoji_count > 0 else 0.0
        
        # Excessive emoji density (more than 1 emoji per 10 chars)
        emoji_density = emoji_count / max(len(content), 1)
        density_score = min(1.0, emoji_density * 2.5) if emoji_density > 0.05 else 0.0
        
        # If more than 3 spam emojis, it's highly suspicious
        if spam_emoji_count > 3:
            return min(1.0, 0.7 + (spam_emoji_count - 3) * 0.1)
        
        return max(spam_emoji_ratio * 0.8, density_score * 0.5)

    def _check_url_flooding(self, content: str) -> float:
        """Check for excessive URLs."""
        url_count = len(URL_PATTERN.findall(content))
        
        if url_count == 0:
            return 0.0
        
        # More than 2 URLs = suspicious
        if url_count > 2:
            return min(1.0, 0.3 + (url_count - 2) * 0.2)
        
        return min(1.0, url_count * 0.2)

    def _check_mention_spam(self, content: str) -> float:
        """Check for mention spam (too many @mentions)."""
        mention_count = len(MENTION_PATTERN.findall(content))
        
        if mention_count == 0:
            return 0.0
        
        # More than 5 mentions = very suspicious
        if mention_count > 5:
            return min(1.0, 0.6 + (mention_count - 5) * 0.08)
        elif mention_count > 3:
            return 0.5
        
        return min(1.0, mention_count * 0.15)

    def _check_hashtag_spam(self, content: str) -> float:
        """Check for hashtag spam (too many hashtags)."""
        hashtag_count = len(HASHTAG_PATTERN.findall(content))
        
        if hashtag_count == 0:
            return 0.0
        
        # More than 10 hashtags = suspicious
        if hashtag_count > 10:
            return min(1.0, 0.5 + (hashtag_count - 10) * 0.05)
        elif hashtag_count > 5:
            return min(1.0, 0.3 + (hashtag_count - 5) * 0.06)
        
        return min(1.0, hashtag_count * 0.06)

    def _check_spam_keywords(self, content_lower: str) -> float:
        """Check for common spam keywords."""
        matched_keywords = sum(
            1 for keyword in SPAM_KEYWORDS if keyword in content_lower
        )
        
        if matched_keywords == 0:
            return 0.0
        
        # Each keyword adds more to score
        return min(1.0, matched_keywords * 0.35)

    def _check_scam_patterns(self, content_lower: str) -> float:
        """Check for crypto scam/rug pull patterns."""
        matched_patterns = sum(
            1 for pattern in SCAM_PATTERNS if pattern in content_lower
        )
        
        if matched_patterns == 0:
            return 0.0
        
        # Scam patterns are high confidence signals
        return min(1.0, matched_patterns * 0.5)

    def _check_bot_keywords(self, content_lower: str) -> float:
        """Check for bot-related keywords."""
        matched_keywords = sum(
            1 for keyword in BOT_KEYWORDS if keyword in content_lower
        )
        
        if matched_keywords == 0:
            return 0.0
        
        # Bot keywords are strong indicators
        return min(1.0, matched_keywords * 0.40)

    def _check_username_pattern(self, username: str) -> float:
        """Check for suspicious username patterns."""
        if not username:
            return 0.5  # Unknown username is suspicious
        
        username_lower = username.lower()
        score = 0.0
        
        # Numeric-heavy usernames (bot-like)
        digit_count = sum(1 for c in username if c.isdigit())
        if digit_count / max(len(username), 1) > 0.5:
            score += 0.4
        
        # Generic bot names
        bot_indicators = ["bot", "auto", "trader", "signal", "alert", "crypto", "trading", "pump", "moon"]
        if any(bot_pattern in username_lower for bot_pattern in bot_indicators):
            score += 0.5
        
        # Repeated characters (aaa, zzz, etc)
        if re.search(r'(.)\1{3,}', username):
            score += 0.3
        
        # All caps or all lowercase with numbers
        if username.isupper() and digit_count > 0:
            score += 0.2
        
        return min(1.0, score)

    def _check_engagement_anomaly(
        self,
        engagement_score: int,
        author_weight: float,
        content_len: int,
    ) -> float:
        """Check for engagement anomalies."""
        # Low weight (not whale) accounts shouldn't have extreme engagement on short content
        if author_weight < 2.0:
            # High engagement on very short content = suspicious (bot behavior)
            if content_len < 50 and engagement_score > 1000:
                return 0.8
            elif content_len < 100 and engagement_score > 2000:
                return 0.6
            elif engagement_score > 5000:
                return 0.7
        
        # Very low engagement on promotional content = also suspicious
        if engagement_score < 5 and content_len > 100:
            return 0.3
        
        return 0.0

    def _check_content_patterns(self, content_lower: str) -> float:
        """Check for bot-like content patterns."""
        score = 0.0

        # Price update patterns (common in trading bots)
        if re.search(r'price.*update|btc.*\$|eth.*\$|crypto.*\$', content_lower):
            score += 0.4

        # Trading signal patterns
        if re.search(r'signal|short|long|buy|sell.*\$[A-Z]+', content_lower):
            score += 0.3

        # Time-based patterns (bots often include timestamps)
        if re.search(r'\d{1,2}:\d{2}|\d{4}-\d{2}-\d{2}', content_lower):
            score += 0.2

        # Data source patterns
        if re.search(r'data.*source|api|binance|coinbase', content_lower):
            score += 0.3

        # Automated/formulaic language
        if re.search(r'automatic|generated|algorithm|robot', content_lower):
            score += 0.5

        return min(1.0, score)

    def filter_batch(self, tweets: list[dict]) -> tuple[list[dict], list[dict]]:
        """
        Filter a batch of tweets, separating clean from spam/bot.

        Args:
            tweets: List of tweet dictionaries with keys:
                - content or text
                - username
                - engagement_score (optional)
                - author_weight (optional)

        Returns:
            (clean_tweets, spam_tweets) tuple
        """
        clean_tweets = []
        spam_tweets = []

        for tweet in tweets:
            content = tweet.get("content") or tweet.get("text", "")
            username = tweet.get("username", "")
            engagement_score = tweet.get("engagement_score", 0)
            author_weight = tweet.get("author_weight", 1.0)

            result = self.detect_spam(content, username, engagement_score, author_weight)

            # Add result metadata
            tweet_copy = dict(tweet)
            tweet_copy["spam_score"] = result.total_score
            tweet_copy["bot_score"] = min(1.0, sum(
                result.content_patterns.get(k, 0.0) 
                for k in ["bot_keywords", "username_pattern"]
            ))
            tweet_copy["is_spam"] = result.is_spam
            tweet_copy["is_bot"] = result.is_bot
            tweet_copy["filter_reasons"] = result.reasons

            if result.is_spam or result.is_bot:
                spam_tweets.append(tweet_copy)
            else:
                clean_tweets.append(tweet_copy)

        return clean_tweets, spam_tweets
