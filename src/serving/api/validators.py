"""Shared request-parameter validators."""

# Trading-pair format: BASE/QUOTE, uppercase letters only.
# Base: 2-6 letters (BTC, ETH, DOGE, AVAX, MATIC, SHIBAF …)
# Quote: 3-5 letters (USD, USDT, USDC, BUSD, BTC)
SYMBOL_PATTERN = r"^[A-Z]{2,6}/[A-Z]{3,5}$"
