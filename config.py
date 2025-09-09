import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_URL = os.getenv("DB_URL", "sqlite:///celestiabridge.db")
OTEL_METRICS_URL = os.getenv("OTEL_METRICS_URL", "https://fdp-lunar.celestia.observer/metrics")
GEO_CSV_PATH = os.getenv("GEO_CSV_PATH", "./context/peers_geo_mainnet_latest.csv")

# Cosmos API base URL
COSMOS_API_BASE_URL = os.getenv("COSMOS_API_BASE_URL", "https://api-celestia-mainnet.validexis.com")

# API endpoints (автоматично формуються з базової URL)
POOL_URL = f"{COSMOS_API_BASE_URL}/cosmos/staking/v1beta1/pool"
SLASHING_URL = f"{COSMOS_API_BASE_URL}/cosmos/slashing/v1beta1/signing_infos?pagination.limit=100"
ANNUAL_PROVISIONS_URL = f"{COSMOS_API_BASE_URL}/cosmos/mint/v1beta1/annual_provisions"
SUPPLY_URL = f"{COSMOS_API_BASE_URL}/cosmos/bank/v1beta1/supply/by_denom?denom=utia"
VALIDATORS_URL = f"{COSMOS_API_BASE_URL}/cosmos/staking/v1beta1/validators?pagination.limit=1000"

# Зовнішні API (залишаються як є)
API_DELEGATORS_URL = "https://celestia-api.polkachu.com/cosmos/staking/v1beta1/validators/{}/delegations?pagination.count_total=true"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price?ids=celestia&vs_currencies=usd"
GITHUB_RELEASES_URL = "https://api.github.com/repos/celestiaorg/celestia-node/releases"
API_TOTAL_DELEGATORS_URL = "https://celestia-api.polkachu.com/cosmos/staking/v1beta1/delegations?pagination.count_total=true"

# LLM API налаштування
GROK_API_KEY = os.getenv("GROK_API_KEY", "")
GROK_API_BASE_URL = os.getenv("GROK_API_BASE_URL", "https://api.x.ai/v1")
GROK_MODEL = os.getenv("GROK_MODEL", "grok-3-mini")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")  # "grok" or "gemini"

VALOPER_ADDRESSES = os.getenv("VALOPER_ADDRESSES", "").split(",") if os.getenv("VALOPER_ADDRESSES") else []

# Add more config variables as needed 