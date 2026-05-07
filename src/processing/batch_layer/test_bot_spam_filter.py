import json
from bot_spam_filter import BotSpamFilter, SpamScore


# ============================================================================
# Sample Test Tweets (Mix of clean, spam, and bot tweets)
# ============================================================================

SAMPLE_TWEETS = [
    # Clean Tweets
    {
        "id": "clean_001",
        "content": "$BTC showing strong support at 45k level, looking for breakout soon",
        "username": "crypto_trader",
        "engagement_score": 45,
        "author_weight": 1.0,
        "label": "CLEAN - Normal market analysis",
    },
    {
        "id": "clean_002",
        "content": "Ethereum gas fees are lower today. Good time for transactions. #crypto #eth",
        "username": "blockchain_dev",
        "engagement_score": 12,
        "author_weight": 1.0,
        "label": "CLEAN - Informative content",
    },
    {
        "id": "clean_003",
        "content": "$SOL recovered well from yesterday's dip. Layer2 solutions gaining traction.",
        "username": "market_analyst",
        "engagement_score": 89,
        "author_weight": 2.5,
        "label": "CLEAN - Whale analysis",
    },

    # Spam tweets
    {
        "id": "spam_001",
        "content": "FOLLOW BACK ✅✅✅ Click link in bio for FREE TOKENS 🚀🚀🚀 Limited time offer! DM me NOW",
        "username": "free_tokens_bot",
        "engagement_score": 10000,
        "author_weight": 1.0,
        "label": "SPAM - Follow back + free token scam",
    },
    {
        "id": "spam_002",
        "content": "$BTC PRESALE 🔥🔥🔥 Guaranteed 100x returns! Join whitelist 💰 Act NOW or miss out! bit.ly/xyz tinyurl.com/abc",
        "username": "crypto_presale123",
        "engagement_score": 5000,
        "author_weight": 1.0,
        "label": "SPAM - Scam presale with link spam",
    },
    {
        "id": "spam_003",
        "content": "@user1 @user2 @user3 @user4 @user5 @user6 @user7 CHECK BIO FOR GIVEAWAY 🌙 AIRDROP 💎 STAKE REWARDS ⚡ #moon #lambo #hodl #btc #eth #xrp #sol #bnb #doge",
        "username": "mention_spam_bot",
        "engagement_score": 2000,
        "author_weight": 1.0,
        "label": "SPAM - Mention spam + excessive hashtags",
    },
    {
        "id": "spam_004",
        "content": "Easy Money 💰 Work From Home! Passive Income! Get Rich Quick! https://scamsite.com/pump",
        "username": "mlm_bot_2024",
        "engagement_score": 300,
        "author_weight": 1.0,
        "label": "SPAM - MLM/scam keywords",
    },

    # Bot tweets
    {
        "id": "bot_001",
        "content": "Automated trading signal: $ETH SHORT at 3500. Stop loss 3600. Take profit 3200. Good luck!",
        "username": "trading_bot_pro",
        "engagement_score": 150,
        "author_weight": 1.0,
        "label": "BOT - Automated trading signal",
    },
    {
        "id": "bot_002",
        "content": "Price update: BTC is at $46,500. Update time: 14:32:15 UTC. Data source: Binance API",
        "username": "crypto_bot_monitor",
        "engagement_score": 5,
        "author_weight": 1.0,
        "label": "BOT - Scheduled/automated price alert",
    },
    {
        "id": "bot_003",
        "content": "This is an automated message. Engagement pod rules apply. Please follow back.",
        "username": "a1a1a1a1a1",
        "engagement_score": 50,
        "author_weight": 1.0,
        "label": "BOT - Numeric username + automated keywords",
    },

    # Edge Cases
    {
        "id": "edge_001",
        "content": "hi",
        "username": "short_poster",
        "engagement_score": 0,
        "author_weight": 1.0,
        "label": "EDGE - Too short content",
    },
    {
        "id": "edge_002",
        "content": "$BTC $ETH $SOL $DOGE $XRP $BNB $ADA $SHIB $LINK $MATIC amazing coins! 🚀🚀🚀 Get them all!",
        "username": "new_user_2024",
        "engagement_score": 500,
        "author_weight": 1.0,
        "label": "EDGE - Many cashtags + many spam emojis + low weight + high engagement",
    },
]


def print_header(text: str) -> None:
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)


def print_tweet_result(tweet: dict, result: SpamScore) -> None:
    """Pretty print tweet and detection result."""
    print(f"\n📌 Tweet ID: {tweet['id']}")
    print(f"   Label: {tweet['label']}")
    print(f"   Username: {tweet['username']}")
    print(f"   Content: {tweet['content'][:70]}...")
    print(f"   Engagement: {tweet['engagement_score']} | Author weight: {tweet['author_weight']}")

    # Results
    print(f"\n   ✅ Results:")
    print(f"      Spam Score: {result.total_score:.3f} | Is Spam: {result.is_spam}")
    print(f"      Is Bot: {result.is_bot}")

    if result.reasons:
        print(f"      🚩 Reasons ({len(result.reasons)}):")
        for reason in result.reasons:
            print(f"         - {reason}")

    # Pattern scores
    if result.content_patterns:
        print(f"      📊 Pattern Scores:")
        for pattern_name, score in sorted(result.content_patterns.items(), key=lambda x: x[1], reverse=True):
            if score > 0.0:
                print(f"         {pattern_name}: {score:.3f}")

    # Verdict
    if result.is_spam:
        verdict = "🚫 FILTERED AS SPAM"
    elif result.is_bot:
        verdict = "🤖 FLAGGED AS BOT"
    else:
        verdict = "✅ PASS - CLEAN TWEET"
    print(f"\n   🎯 VERDICT: {verdict}")


def test_individual_tweets() -> None:
    """Test each tweet individually."""
    print_header("INDIVIDUAL TWEET DETECTION TEST")

    filter_engine = BotSpamFilter(spam_threshold=0.3, bot_threshold=0.3)
    
    clean_count = 0
    spam_count = 0
    bot_count = 0

    for tweet in SAMPLE_TWEETS:
        result = filter_engine.detect_spam(
            content=tweet["content"],
            username=tweet["username"],
            engagement_score=tweet["engagement_score"],
            author_weight=tweet["author_weight"],
        )

        print_tweet_result(tweet, result)

        if result.is_spam:
            spam_count += 1
        elif result.is_bot:
            bot_count += 1
        else:
            clean_count += 1

    # Summary
    total = len(SAMPLE_TWEETS)
    print_header("DETECTION SUMMARY")
    print(f"\n  Total tweets tested: {total}")
    print(f"  ✅ Clean tweets: {clean_count} ({100*clean_count/total:.1f}%)")
    print(f"  🚫 Spam tweets: {spam_count} ({100*spam_count/total:.1f}%)")
    print(f"  🤖 Bot tweets: {bot_count} ({100*bot_count/total:.1f}%)")


def test_batch_filtering() -> None:
    """Test batch filtering function."""
    print_header("BATCH FILTERING TEST")

    filter_engine = BotSpamFilter(spam_threshold=0.3, bot_threshold=0.3)

    # Prepare tweet list for batch processing
    tweets_for_batch = [
        {
            "content": tweet["content"],
            "username": tweet["username"],
            "engagement_score": tweet["engagement_score"],
            "author_weight": tweet["author_weight"],
            "id": tweet["id"],
            "label": tweet["label"],
        }
        for tweet in SAMPLE_TWEETS
    ]

    clean_tweets, spam_tweets = filter_engine.filter_batch(tweets_for_batch)

    print(f"\n  Input tweets: {len(tweets_for_batch)}")
    print(f"  Output clean: {len(clean_tweets)}")
    print(f"  Output spam/bot: {len(spam_tweets)}")

    print_header("CLEANED TWEETS (Passed Filter)")
    for tweet in clean_tweets:
        print(f"\n  ✅ {tweet['id']}: {tweet['label']}")
        print(f"     Spam score: {tweet['spam_score']:.3f}")

    print_header("FILTERED TWEETS (Spam/Bot)")
    for tweet in spam_tweets:
        print(f"\n  🚫 {tweet['id']}: {tweet['label']}")
        print(f"     Spam score: {tweet['spam_score']:.3f}")
        print(f"     Bot score: {tweet['bot_score']:.3f}")
        print(f"     Is spam: {tweet['is_spam']} | Is bot: {tweet['is_bot']}")
        if tweet["filter_reasons"]:
            print(f"     Reasons: {', '.join(tweet['filter_reasons'][:3])}")


def test_threshold_sensitivity() -> None:
    """Test how different thresholds affect detection."""
    print_header("THRESHOLD SENSITIVITY TEST")

    test_tweet = {
        "content": "$BTC presale limited time! Free airdrop 🚀🔥💰 DM me now",
        "username": "trader123",
        "engagement_score": 200,
        "author_weight": 1.0,
    }

    thresholds = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]

    print(f"\n  Test content: '{test_tweet['content'][:60]}...'")
    print(f"\n  Threshold | Is Spam | Spam Score")
    print("  " + "-" * 35)

    for threshold in thresholds:
        filter_engine = BotSpamFilter(spam_threshold=threshold, bot_threshold=0.3)
        result = filter_engine.detect_spam(**test_tweet)
        is_spam_str = "✅ YES" if result.is_spam else "❌ NO"
        print(f"    {threshold:.1f}    | {is_spam_str:6s} | {result.total_score:.3f}")


def test_sentiment_scoring() -> None:
    """Demo sentiment scoring (using simple lexicon)."""
    print_header("SENTIMENT SCORING DEMO")

    try:
        from sentiment_lexicon import SentimentAnalyzer
        
        analyzer = SentimentAnalyzer()
        
        test_sentences = [
            ("$BTC is amazing! Love crypto!", "Bullish"),
            ("$ETH crashed hard. Terrible day.", "Bearish"),
            ("$SOL is stable. Just holding.", "Neutral"),
            ("Bitcoin went up. Then down. Complex.", "Mixed"),
        ]
        
        print("\n  Sentence | Label | Sentiment Score")
        print("  " + "-" * 55)
        
        for sentence, label in test_sentences:
            score = analyzer.get_score(sentence)
            print(f"  {sentence[:35]:35s} | {label:8s} | {score:6.3f}")
    
    except Exception as e:
        print(f"\n  ⚠️  Sentiment analyzer not available: {e}")
        print("  (You may need to run: pip install nltk)")


def main() -> None:
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "BOT & SPAM FILTER TEST SUITE" + " " * 31 + "║")
    print("╚" + "=" * 78 + "╝")

    test_individual_tweets()
    test_batch_filtering()
    test_threshold_sensitivity()
    test_sentiment_scoring()

    print_header("TEST COMPLETE")
    print("\n  ✅ All tests finished!\n")


if __name__ == "__main__":
    main()
