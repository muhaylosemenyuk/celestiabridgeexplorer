import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_URL = os.getenv("DB_URL", "sqlite:///celestiabridge.db")
OTEL_METRICS_URL = os.getenv("OTEL_METRICS_URL", "https://fdp-lunar.celestia.observer/metrics")
GEO_CSV_PATH = os.getenv("GEO_CSV_PATH", "./context/peers_geo_mainnet_latest.csv")

# API endpoints
POOL_URL = os.getenv("POOL_URL", "https://celestia-mainnet-api.itrocket.net/cosmos/staking/v1beta1/pool")
SLASHING_URL = os.getenv("SLASHING_URL", "https://celestia-mainnet-api.itrocket.net/cosmos/slashing/v1beta1/signing_infos?pagination.limit=100")
ANNUAL_PROVISIONS_URL = os.getenv("ANNUAL_PROVISIONS_URL", "https://celestia-mainnet-api.itrocket.net/cosmos/mint/v1beta1/annual_provisions")
SUPPLY_URL = os.getenv("SUPPLY_URL", "https://celestia-mainnet-api.itrocket.net/cosmos/bank/v1beta1/supply/by_denom?denom=utia")
API_DELEGATORS_URL = os.getenv("API_DELEGATORS_URL", "https://celestia-api.polkachu.com/cosmos/staking/v1beta1/validators/{}/delegations?pagination.count_total=true")
COINGECKO_URL = os.getenv("COINGECKO_URL", "https://api.coingecko.com/api/v3/simple/price?ids=celestia&vs_currencies=usd")
GITHUB_RELEASES_URL = os.getenv("GITHUB_RELEASES_URL", "https://api.github.com/repos/celestiaorg/celestia-node/releases")
VALIDATORS_URL = os.getenv("VALIDATORS_URL", "https://celestia-mainnet-api.itrocket.net/cosmos/staking/v1beta1/validators?pagination.limit=1000")
API_TOTAL_DELEGATORS_URL = os.getenv("API_TOTAL_DELEGATORS_URL", "https://celestia-api.polkachu.com/cosmos/staking/v1beta1/delegations?pagination.count_total=true")

VALOPER_ADDRESSES = os.getenv("VALOPER_ADDRESSES", "").split(",") if os.getenv("VALOPER_ADDRESSES") else []

# Add more config variables as needed 