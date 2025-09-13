# HealCo Lite

HealCo Lite is a Telegram bot that helps users manage nutrition, fitness, and motivation. It integrates with OpenAI and other services to provide dietary advice, workout plans, and simple gamification features.

## Dependencies

Install the project's dependencies with:

```
pip install -r requirements.txt
```

Key packages include:

- python-telegram-bot
- openai
- pydantic
- aiohttp
- python-dotenv
- requests-oauthlib
- flask
- google-generativeai
- httpx
- requests
- telegram

## Running locally

1. Install dependencies as shown above.
2. Set environment variables:

```
export OPENAI_API_KEY=your_openai_key
export TELEGRAM_BOT_TOKEN=your_bot_token
export TELEGRAM_PAYMENT_PROVIDER_TOKEN=your_payment_token
export DEVELOPER_USER_ID=123456789
# Optional integrations
export GOOGLE_CSE_KEY=...
export GOOGLE_CSE_CX=...
export VISION_KEY=...
export USDA_FDC_API_KEY=...
export EXTERNAL_JSONL_URL=...
export GDRIVE_ID=...
export FATSECRET_KEY=...
export FATSECRET_SECRET=...
export GEMINI_API_KEY=...
```

3. Run the bot:

```
python main.py
```

## Railway deployment

1. Create a new Railway project and upload this repository.
2. In the "Variables" section, configure the same environment variables shown above.
3. Set the start command to:

```
python main.py
```

The service will start using the provided configuration.

