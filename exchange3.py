import ccxt
import pandas as pd
import logging
import os
import time

# Set up logging for better feedback
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---

# Expanded list of cryptocurrencies to check, simulating a "top 1000" breadth.
# This list will be filtered for stablecoins programmatically.
RAW_SUPPORTED_CRYPTOS = [
    'BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'TRX', 'DOGE', 'ADA', 'LTC', 'DOT',
    'LINK', 'UNI', 'BCH', 'XLM', 'VET', 'EOS', 'XTZ', 'NEO', 'ATOM', 'ETC',
    'ZEC', 'DASH', 'XMR', 'ALGO', 'FIL', 'ICP', 'GRT', 'AAVE', 'COMP', 'SNX',
    'PEPE', 'BONK', 'FLOKI', 'SEI', 'PYTH', 'MANA', 'FTN', 'LQTY', 'WAVES',
    'SHIB', 'AVAX', 'MATIC', 'LEO', 'TON', 'NEAR', 'APT', 'IMX', 'SUI', 'ARB',
    'OP', 'GRT', 'FET', 'RNDR', 'INJ', 'TIA', 'MINA', 'FTM', 'KAS', 'EGLD',
    'SATS', 'ORDI', 'WIF', 'JUP', 'ENA', 'W', 'BOME', 'ONDO', 'WLD', 'CORE',
    'PENDLE', 'ALT', 'STRK', 'PIXEL', 'AEVO', 'JTO', 'TNSR', 'ETHFI', 'NFP',
    'MNT', 'OKB', 'CRO', 'STX', 'HBAR', 'FLR', 'KSM', 'THETA', 'AXS', 'SAND',
    'CHZ', 'GALA', 'ENJ', 'ZIL', 'BAT', 'CRV', 'SUSHI', 'YFI', 'MKR',
    'CELO', 'FLOW', 'EVMOS', 'OSMO', 'KAVA', 'ROSE', 'ONE', 'ICX', 'RVN', 'CELR',
    'ANKR', 'OMG', 'ZRX', 'KNC', 'BAL', 'REN', 'BAND', 'OCEAN', 'RLC', 'NMR',
    'CVC', 'DGB', 'AR', 'AUDIO', 'BADGER', 'BAKE', 'BEL', 'BLZ', 'BTT', 'C98',
    'CTSI', 'DODO', 'DUSK', 'DYDX', 'FLM', 'FORTH',
    'FTT', 'GTC', 'HNT', 'HOT', 'IOST', 'JASMY', 'KDA', 'LPT', 'LRC',
    'MASK', 'MDX', 'MIR', 'MTL', 'NKN', 'OGN', 'ONT', 'PHA', 'POWR', 'PUNDIX',
    'QTUM', 'REEF', 'RSR', 'RUNE', 'SC', 'SKL', 'SRM', 'STPT', 'STORJ', 'SUN',
    'SUPER', 'SXP', 'TFUEL', 'TLM', 'TRB', 'UMA', 'UNFI', 'VTHO', 'WAN', 'WING',
    'XEC', 'XYM', 'YGG', 'ZIL', 'CTXC', 'DATA', 'DENT', 'DREP', 'FUN', 'GOLEM',
    'MBL', 'NULS', 'PERL', 'PNT', 'POND', 'PROM', 'QNT', 'RBN', 'REQ', 'RIF',
    'RING', 'RLY', 'SAFEMOON', 'SFP', 'SISHIB', 'SLP', 'SOLVE', 'STEEM', 'STG',
    'STMX', 'SWFTC', 'SYS', 'TORN', 'TRU', 'UFT', 'VITE', 'VOXEL', 'WTC', 'XVS',
    'ZKS', 'ASTR', 'GLMR', 'MOVR', 'KDA', 'MINA', 'ONE', 'QTUM', 'RVN', 'SC',
    'STX', 'XEM', 'ZIL', 'ZEN', 'ICX', 'IOST', 'ONG', 'VET', 'WAN', 'WAVES',
    'XLM', 'XRP', 'XTZ', 'ZRX', 'CELR', 'ANKR', 'BAND', 'BAT', 'BCH', 'COMP',
    'CRV', 'DASH', 'DGB', 'DOGE', 'DOT', 'ENJ', 'EOS', 'ETC', 'ETH', 'FIL',
    'GALA', 'GRT', 'ICP', 'LINK', 'LTC', 'MATIC', 'NEO', 'OMG', 'ONT', 'SNX',
    'SOL', 'THETA', 'TRX', 'UNI', 'XMR', 'ZEC', 'BNB', 'BTC', 'ADA', 'AVAX',
    'SHIB', 'LEO', 'TON', 'NEAR', 'FTM', 'KAS', 'EGLD', 'SATS', 'ORDI', 'WIF',
    'JUP', 'ENA', 'W', 'BOME', 'ONDO', 'WLD', 'CORE', 'PENDLE', 'ALT', 'STRK',
    'PIXEL', 'AEVO', 'JTO', 'TNSR', 'ETHFI', 'NFP', 'MNT', 'OKB', 'CRO', 'STX',
    'HBAR', 'FLR', 'KSM', 'AXS', 'SAND', 'CHZ', 'GALA', 'ENJ', 'ZIL', 'BAT',
    'CRV', 'SUSHI', 'YFI', 'MKR', 'CELO', 'FLOW', 'EVMOS', 'OSMO', 'KAVA',
    'ROSE', 'ONE', 'ICX', 'RVN', 'CELR', 'ANKR', 'OMG', 'ZRX', 'KNC', 'BAL',
    'REN', 'BAND', 'OCEAN', 'RLC', 'NMR', 'CVC', 'DGB', 'AR', 'AUDIO', 'BADGER',
    'BAKE', 'BEL', 'BLZ', 'BTT', 'C98', 'CTSI', 'DODO', 'DUSK', 'DYDX', 'FLM',
    'FORTH', 'FTT', 'GTC', 'HNT', 'HOT', 'IOST', 'JASMY', 'KDA', 'LPT', 'LRC',
    'MASK', 'MDX', 'MIR', 'MTL', 'NKN', 'OGN', 'ONT', 'PHA', 'POWR', 'PUNDIX',
    'QTUM', 'REEF', 'RSR', 'RUNE', 'SC', 'SKL', 'SRM', 'STPT', 'STORJ', 'SUN',
    'SUPER', 'SXP', 'TFUEL', 'TLM', 'TRB', 'UMA', 'UNFI', 'VTHO', 'WAN', 'WING',
    'XEC', 'XYM', 'YGG', 'ZIL', 'CTXC', 'DATA', 'DENT', 'DREP', 'FUN', 'GOLEM',
    'MBL', 'NULS', 'PERL', 'PNT', 'POND', 'PROM', 'QNT', 'RBN', 'REQ', 'RIF',
    'RING', 'RLY', 'SAFEMOON', 'SFP', 'SISHIB', 'SLP', 'SOLVE', 'STEEM', 'STG',
    'STMX', 'SWFTC', 'SYS', 'TORN', 'TRU', 'UFT', 'VITE', 'VOXEL', 'WTC', 'XVS',
    'ZKS',
    '1INCH', 'AAVE', 'ACA', 'ACH', 'ACM', 'AERGO', 'AGLD', 'AKRO', 'ALCX', 'ALICE',
    'ALPHA', 'ALPINE', 'AMP', 'ANC', 'ANT', 'ANY', 'API3', 'APX', 'AR', 'ARDR',
    'ARK', 'ARPA', 'ASR', 'AST', 'ATA', 'ATLAS', 'AUDIO', 'AUTO', 'AVA', 'BADGER',
    'BAKE', 'BAL', 'BAND', 'BAR', 'BAT', 'BETA', 'BICO', 'BIFI', 'BLZ', 'BNT',
    'BOND', 'BSW', 'BURGER', 'C98', 'CAKE', 'CELO', 'CFX', 'CHZ', 'CITY', 'CLV',
    'COCOS', 'COMBO', 'CONV', 'COS', 'COTI', 'CRV', 'CTK', 'CTSI', 'CVC', 'CVX',
    'DCR', 'DENT', 'DGB', 'DODO', 'DREP', 'DUSK', 'DYDX', 'ENJ', 'ENS', 'ERN',
    'FIDA', 'FIL', 'FIRO', 'FLM', 'FLOW', 'FORTH', 'FRONT', 'FTM', 'FXS', 'GALA',
    'GHST', 'GLM', 'GMT', 'GNO', 'GRT', 'GTC', 'HBAR', 'HFT', 'HIVE', 'HOT',
    'ICP', 'IDEX', 'ILV', 'IMX', 'INJ', 'IOST', 'IOTA', 'JASMY', 'JOE', 'JST',
    'KAVA', 'KDA', 'KEEP', 'KEY', 'KNC', 'KP3R', 'KSM', 'LAZIO', 'LINA', 'LPT',
    'LRC', 'LSK', 'LTO', 'LUNA', 'MANA', 'MASK', 'MATIC', 'MDX', 'MINA', 'MIR',
    'MKR', 'MLN', 'MOVR', 'MTL', 'MULTI', 'NEAR', 'NEO', 'NEXO', 'NKN', 'NMR',
    'OCEAN', 'OGN', 'OMG', 'ONE', 'ONT', 'OXT', 'PAXG', 'PERP', 'PHA', 'PHB',
    'PIRATE', 'PLA', 'PNT', 'POLS', 'POWR', 'PROM', 'PSG', 'PUNDIX', 'QNT', 'QTUM',
    'RAD', 'RARE', 'REEF', 'REN', 'REQ', 'RNDR', 'ROSE', 'RSR', 'RUNE', 'RVN',
    'SAND', 'SC', 'SCRT', 'SFP', 'SKL', 'SLP', 'SNX', 'SOL', 'SPELL', 'SRM',
    'STG', 'STMX', 'STORJ', 'STRAX', 'STRK', 'SUN', 'SUPER', 'SUSHI', 'SXP', 'SYS',
    'TFUEL', 'THETA', 'TKO', 'TLM', 'TRB', 'TRU', 'UMA', 'UNFI', 'VGX', 'VTHO',
    'WAN', 'WAVES', 'WAXP', 'WING', 'WOO', 'XEC', 'XEM', 'XLM', 'XMR', 'XYM',
    'YFI', 'YGG', 'ZEC', 'ZEN', 'ZIL', 'ZRX', 'ACE', 'AGI', 'AKT', 'ALT', 'AMP',
    'AR', 'ARKM', 'ARPA', 'ATA', 'AUCTION', 'AUDIO', 'AXL', 'BADGER', 'BAKE', 'BAL',
    'BAND', 'BAT', 'BCH', 'BEL', 'BETA', 'BICO', 'BIFI', 'BLUR', 'BLZ', 'BNT',
    'BOND', 'BSW', 'BURGER', 'C98', 'CAKE', 'CELO', 'CFX', 'CHR', 'CHZ', 'CKB',
    'CLV', 'COCOS', 'COMBO', 'CONV', 'COS', 'COTI', 'CRV', 'CTK', 'CTSI', 'CVC',
    'CVX', 'DCR', 'DENT', 'DGB', 'DODO', 'DREP', 'DUSK', 'DYDX', 'EGLD', 'ENJ',
    'ENS', 'ERN', 'FET', 'FIDA', 'FIL', 'FIRO', 'FLM', 'FLOW', 'FORTH', 'FXS',
    'GALA', 'GHST', 'GLM', 'GMT', 'GNO', 'GRT', 'GTC', 'HBAR', 'HFT', 'HIVE',
    'HOT', 'ICP', 'IDEX', 'ILV', 'IMX', 'INJ', 'IOST', 'IOTA', 'JASMY', 'JOE',
    'JST', 'KAVA', 'KDA', 'KEEP', 'KEY', 'KNC', 'KP3R', 'KSM', 'LAZIO', 'LINA',
    'LPT', 'LRC', 'LSK', 'LTO', 'LUNA', 'MANA', 'MASK', 'MATIC', 'MDX', 'MINA',
    'MIR', 'MKR', 'MLN', 'MOVR', 'MTL', 'MULTI', 'NEAR', 'NEO', 'NEXO', 'NKN',
    'NMR', 'OCEAN', 'OGN', 'OMG', 'ONE', 'ONT', 'OXT', 'PAXG', 'PERP', 'PHA',
    'PHB', 'PIRATE', 'PLA', 'PNT', 'POLS', 'POWR', 'PROM', 'PSG', 'PUNDIX', 'QNT',
    'QTUM', 'RAD', 'RARE', 'REEF', 'REN', 'REQ', 'RNDR', 'ROSE', 'RSR', 'RUNE',
    'RVN', 'SAND', 'SC', 'SCRT', 'SFP', 'SKL', 'SLP', 'SNX', 'SOL', 'SPELL',
    'SRM', 'STG', 'STMX', 'STORJ', 'STRAX', 'STRK', 'SUN', 'SUPER', 'SUSHI', 'SXP',
    'SYS', 'TFUEL', 'THETA', 'TKO', 'TLM', 'TRB', 'TRU', 'UMA', 'UNFI', 'VGX',
    'VTHO', 'WAN', 'WAVES', 'WAXP', 'WING', 'WOO', 'XEC', 'XEM', 'XLM', 'XMR',
    'XYM', 'YFI', 'YGG', 'ZEC', 'ZEN', 'ZIL', 'ZRX','AION', 'ARDR', 'ARK', 
    'BTS', 'CLOAK', 'DCR', 'DGB', 'DGD', 'DNT', 'EDG',
    'EMC', 'ENJ', 'FCT', 'GAME', 'GAS', 'GNT', 'GRC', 'ICN', 'IOC', 'KMD',
    'LSK', 'MAID', 'MLN', 'MTL', 'NAV', 'NXT', 'OMG', 'PART', 'PIVX', 'POE',
    'POLY', 'POWR', 'PPT', 'QSP', 'RDD', 'REP', 'RLC', 'SALT', 'SNGLS', 'STEEM',
    'STORJ', 'STRAT', 'SYS', 'VTC', 'WINGS', 'XCP', 'XEM', 'XMR', 'XVG', 'ZCL',
    'ZRX', 'ADX', 'AE', 'AGI', 'AION', 'AMB', 'APPC', 'ARN', 'AST', 'BAT',
    'BCD', 'BCPT', 'BLZ', 'BNT', 'BQX', 'BRD', 'BTS', 'CDT', 'CHAT', 'CLOAK',
    'CMT', 'CND', 'CVC', 'DASH', 'DATA', 'DCR', 'DGD', 'DLT', 'DNT', 'EDO',
    'ELF', 'ENG', 'ENJ', 'EOS', 'ETC', 'EVX', 'FCT', 'FUEL', 'FUN', 'GAS',
    'GNT', 'GRS', 'GTO', 'GVT', 'GXS', 'HSR', 'ICN', 'ICX', 'INS', 'IOST',
    'IOTA', 'ITC', 'KMD', 'LEND', 'LINK', 'LRC', 'LUN', 'MAID', 'MANA', 'MCO',
    'MDA', 'MFT', 'MITH', 'MTH', 'MTL', 'NANO', 'NAV', 'NCASH', 'NEBL', 'NEM',
    'NEXO', 'NULS', 'NXS', 'OAX', 'OMG', 'ONT', 'OST', 'POA', 'POE', 'POLY',
    'POWR', 'PPT', 'PST', 'QKC', 'QLC', 'QSP', 'QTUM', 'RCN', 'RDD', 'RDN',
    'REN', 'REP', 'REQ', 'RHOC', 'RLC', 'RPX', 'SALT', 'SAN', 'SNM', 'SNT',
    'STEEM', 'STORM', 'STORJ', 'STRAT', 'SUB', 'SYS', 'TNB', 'TNT', 'TRIG',
    'TRX', 'VIB', 'VIBE', 'WABI', 'WAN', 'WAVES', 'WINGS', 'WTC', 'XEM', 'XLM',
    'XMR', 'XRP', 'XVG', 'XZC', 'YOYO', 'ZEC', 'ZEN', 'ZIL', 'ZRX', 'ZSC',
    'AOA', 'ARPA', 'ATOM', 'BTT', 'CELR', 'CHZ', 'COS', 'DENT', 'DOGE', 'DREP',
    'FET', 'FTM', 'HBAR', 'HOT', 'IOST', 'JST', 'KAVA', 'ONE', 'ONT', 'RVN',
    'STX', 'TFUEL', 'THETA', 'VET', 'WIN', 'XLM', 'XTZ', 'ZIL', 'ZRX', 'ALGO',
    'BAND', 'BAT', 'BCH', 'BNB', 'BTC', 'ADA', 'COMP', 'CRV', 'DASH', 'DOT',
    'ENJ', 'EOS', 'ETC', 'ETH', 'FIL', 'GALA', 'GRT', 'ICP', 'LINK', 'LTC',
    'MATIC', 'MKR', 'NEAR', 'NEO', 'OMG', 'REN', 'SNX', 'SOL', 'SUSHI', 'TRX',
    'UNI', 'XRP', 'ZEC', 'AAVE', 'AXS', 'CHR', 'ENJ', 'GALA', 'MANA', 'SAND',
    'SLP', 'TLM', 'YGG', 'DREP', 'FUN', 'GOLEM', 'MBL', 'NULS', 'PERL', 'PNT',
    'POND', 'PROM', 'QNT', 'RBN', 'REQ', 'RIF', 'RING', 'RLY', 'SAFEMOON', 'SFP',
    'SISHIB', 'SLP', 'SOLVE', 'STEEM', 'STG', 'STMX', 'SWFTC', 'SYS', 'TORN',
    'TRU', 'UFT', 'VITE', 'VOXEL', 'WTC', 'XVS', 'ZKS', 'ASTR', 'GLMR', 'MOVR',
    'KDA', 'MINA', 'ONE', 'QTUM', 'RVN', 'SC', 'STX', 'XEM', 'ZIL', 'ZEN', 'ICX',
    'IOST', 'ONG', 'VET', 'WAN', 'WAVES', 'XLM', 'XRP', 'XTZ', 'ZRX', 'CELR',
    'ANKR', 'BAND', 'BAT', 'BCH', 'COMP', 'CRV', 'DASH', 'DGB', 'DOGE', 'DOT',
    'ENJ', 'EOS', 'ETC', 'ETH', 'FIL', 'GALA', 'GRT', 'ICP', 'LINK', 'LTC',
    'MATIC', 'NEO', 'OMG', 'ONT', 'SNX', 'SOL', 'THETA', 'TRX', 'UNI', 'XMR',
    'ZEC', 'BNB', 'BTC', 'ADA', 'AVAX', 'SHIB', 'LEO', 'TON', 'NEAR', 'FTM',
    'KAS', 'EGLD', 'SATS', 'ORDI', 'WIF', 'JUP', 'ENA', 'W', 'BOME', 'ONDO',
    'WLD', 'CORE', 'PENDLE', 'ALT', 'STRK', 'PIXEL', 'AEVO', 'JTO', 'TNSR',
    'ETHFI', 'NFP', 'MNT', 'OKB', 'CRO', 'STX', 'HBAR', 'FLR', 'KSM', 'AXS',
    'SAND', 'CHZ', 'GALA', 'ENJ', 'ZIL', 'BAT', 'CRV', 'SUSHI', 'YFI', 'MKR',
    'CELO', 'FLOW', 'EVMOS', 'OSMO', 'KAVA', 'ROSE', 'ONE', 'ICX', 'RVN', 'CELR',
    'ANKR', 'OMG', 'ZRX', 'KNC', 'BAL', 'REN', 'BAND', 'OCEAN', 'RLC', 'NMR',
    'CVC', 'DGB', 'AR', 'AUDIO', 'BADGER', 'BAKE', 'BEL', 'BLZ', 'BTT', 'C98',
    'CTSI', 'DODO', 'DUSK', 'DYDX', 'FLM', 'FORTH', 'FTT', 'GTC', 'HNT', 'HOT',
    'IOST', 'JASMY', 'KDA', 'LPT', 'LRC', 'MASK', 'MDX', 'MIR', 'MTL', 'NKN',
    'OGN', 'ONT', 'PHA', 'POWR', 'PUNDIX', 'QTUM', 'REEF', 'RSR', 'RUNE', 'SC',
    'SKL', 'SRM', 'STPT', 'STORJ', 'SUN', 'SUPER', 'SXP', 'TFUEL', 'TLM', 'TRB',
    'UMA', 'UNFI', 'VTHO', 'WAN', 'WING', 'XEC', 'XYM', 'YGG', 'ZIL', 'CTXC',
    'DATA', 'DENT', 'DREP', 'FUN', 'GOLEM', 'MBL', 'NULS', 'PERL', 'PNT', 'POND',
    'PROM', 'QNT', 'RBN', 'REQ', 'RIF', 'RING', 'RLY', 'SAFEMOON', 'SFP',
    'SISHIB', 'SLP', 'SOLVE', 'STEEM', 'STG', 'STMX', 'SWFTC', 'SYS', 'TORN',
    'TRU', 'UFT', 'VITE', 'VOXEL', 'WTC', 'XVS', 'ZKS'
]

# Remove duplicates and sort for consistency
RAW_SUPPORTED_CRYPTOS = list(sorted(list(set(RAW_SUPPORTED_CRYPTOS))))

# List of common stablecoin symbols to filter out
STABLECOINS = [
    'USDT', 'USDC', 'BUSD', 'DAI', 'FRAX', 'TUSD', 'USTC', 'PAXG', 'CUSD', 'EURT',
    'USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'CNY', 'HKD', 'NZD', 'SEK',
    'SGD', 'THB', 'TRY', 'ZAR', 'MXN', 'RUB', 'INR', 'BRL', 'ARS', 'CLP', 'COP',
    'PEN', 'PHP', 'PLN', 'RON', 'VND', 'IDR', 'KRW', 'MYR', 'NGN', 'PKR', 'UAH',
    'VND', 'XOF', 'XPF', 'XAF', 'XCD', 'XDR', 'XAU', 'XAG', 'XPT', 'XPD', # Fiat and precious metals often used as base
    'USDP', 'GUSD', 'HUSD', 'SUSD', 'EURS', 'CUSDT', 'CUSDC', 'CEUR', 'CHFR', 'CJPY', # More stablecoins
    'BKRW', 'JPM', 'MUSD', 'QCAD', 'QCASH', 'RSV', 'SAGA', 'TRYB', 'VNDC', 'WUSD',
    'XSGD', 'ZUSD'
]

def filter_stablecoins(crypto_list):
    """Removes stablecoins from a list of cryptocurrency symbols."""
    return [crypto for crypto in crypto_list if crypto.upper() not in STABLECOINS]

# Apply stablecoin filtering
SUPPORTED_CRYPTOS = filter_stablecoins(RAW_SUPPORTED_CRYPTOS)

# List of CEX exchanges to check. DEX exchanges are excluded.
EXCHANGES_TO_CHECK = [
    {'id': 'binance', 'name': 'Binance'},
    {'id': 'coinbase', 'name': 'Coinbase'},
    {'id': 'kraken', 'name': 'Kraken'},
    {'id': 'bitget', 'name': 'Bitget'},
    {'id': 'bybit', 'name': 'Bybit'},
    {'id': 'kucoin', 'name': 'KuCoin'},
    {'id': 'bitfinex', 'name': 'Bitfinex'},
    {'id': 'cryptocom', 'name': 'Crypto.com'},
    {'id': 'gateio', 'name': 'Gate.io'},
    {'id': 'huobi', 'name': 'Huobi'},
    {'id': 'mexc', 'name': 'MEXC'},
    {'id': 'bitstamp', 'name': 'Bitstamp'},
    {'id': 'lbank', 'name': 'LBank'},
    {'id': 'coinex', 'name': 'CoinEx'},
    {'id': 'ascendex', 'name': 'AscendEX'},
    {'id': 'digifinex', 'name': 'DigiFinex'},
    {'id': 'fmfwio', 'name': 'FMFW.io'},
    {'id': 'hitbtc', 'name': 'HitBTC'},
    {'id': 'phemex', 'name': 'Phemex'},
    {'id': 'probit', 'name': 'ProBit Global'},
    {'id': 'whitebit', 'name': 'WhiteBIT'},
    {'id': 'woo', 'name': 'WOO X'},
    {'id': 'poloniex', 'name': 'Poloniex'}, # Added more
    {'id': 'bitrue', 'name': 'Bitrue'}, # Added more
    {'id': 'tokocrypto', 'name': 'Tokocrypto'}, # Added more
    {'id': 'indodax', 'name': 'Indodax'}, # Added more
    {'id': 'upbit', 'name': 'Upbit'}, # Added more
]
unique_exchanges = []
seen_ids = set()
for ex in EXCHANGES_TO_CHECK:
    if ex['id'] == 'binanceus': # Explicitly skip binanceus
        continue
    if ex['id'] not in seen_ids:
        unique_exchanges.append(ex)
        seen_ids.add(ex['id'])
EXCHANGES_TO_CHECK = unique_exchanges


# Base currencies to check for trading pairs (e.g., BTC/USDT, ETH/USD)
# Prioritize USDT as it's most common for spot.
BASE_CURRENCIES = ['USDT', 'USD', 'USDC', 'BUSD', 'DAI']

# --- Main Logic ---

def get_exchange_crypto_support():
    """
    Scrapes each configured exchange to determine which cryptocurrencies
    are supported as spot trading pairs.
    Returns a dictionary of dictionaries: {'exchange_id': {'crypto_symbol': True/False}}
    """
    exchange_support_data = {ex['id']: {} for ex in EXCHANGES_TO_CHECK}

    for ex_config in EXCHANGES_TO_CHECK:
        exchange_id = ex_config['id']
        exchange_name = ex_config['name']
        logger.info(f"Checking support for {exchange_name} ({exchange_id})...")

        try:
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({
                'enableRateLimit': True,
                'timeout': 30000, # 30 seconds timeout
            })
            exchange.load_markets()
            logger.info(f"Markets loaded for {exchange_name}.")

            for crypto in SUPPORTED_CRYPTOS:
                is_supported = False
                for base_currency in BASE_CURRENCIES:
                    symbol_candidate = f"{crypto}/{base_currency}"
                    if symbol_candidate in exchange.markets:
                        market = exchange.markets[symbol_candidate]
                        if market['spot']: # Ensure it's a spot market
                            is_supported = True
                            break # Found a spot market for this crypto
                exchange_support_data[exchange_id][crypto] = is_supported
            
        except ccxt.ExchangeNotAvailable as e:
            logger.warning(f"Exchange {exchange_name} is not available: {str(e)}")
            for crypto in SUPPORTED_CRYPTOS:
                exchange_support_data[exchange_id][crypto] = False # Mark all as unsupported if exchange is down
        except ccxt.NetworkError as e:
            logger.warning(f"Network error with {exchange_name}: {str(e)}")
            for crypto in SUPPORTED_CRYPTOS:
                exchange_support_data[exchange_id][crypto] = False
        except ccxt.DDoSProtection as e:
            logger.warning(f"DDoS Protection for {exchange_name}: {str(e)}")
            for crypto in SUPPORTED_CRYPTOS:
                exchange_support_data[exchange_id][crypto] = False
        except ccxt.RequestTimeout as e:
            logger.warning(f"Request Timeout for {exchange_name}: {str(e)}")
            for crypto in SUPPORTED_CRYPTOS:
                exchange_support_data[exchange_id][crypto] = False
        except Exception as e:
            logger.error(f"An unexpected error occurred with {exchange_name}: {type(e).__name__} - {str(e)}")
            for crypto in SUPPORTED_CRYPTOS:
                exchange_support_data[exchange_id][crypto] = False
        
        time.sleep(1) # Small delay between exchanges to be polite and avoid rate limits

    return exchange_support_data

def generate_excel_report(support_data):
    """
    Generates an Excel spreadsheet from the crypto support data.
    """
    logger.info("Generating Excel report...")

    # Create a DataFrame from the support data
    df = pd.DataFrame.from_dict(support_data, orient='index')

    # Transpose the DataFrame to have cryptos as rows and exchanges as columns
    df = df.T

    # Sort columns (exchanges) alphabetically for better readability
    # Get the actual exchange names from EXCHANGES_TO_CHECK to ensure correct sorting and display
    exchange_names_map = {ex['id']: ex['name'] for ex in EXCHANGES_TO_CHECK}
    # Create a list of column names (exchange IDs) in the desired sorted order (by name)
    sorted_exchange_ids = sorted(df.columns, key=lambda x: exchange_names_map.get(x, x))
    df = df[sorted_exchange_ids]

    # Rename columns from exchange IDs to their full names for display in Excel
    df.rename(columns=exchange_names_map, inplace=True)

    # Map boolean values to checkmark/cross symbols
    df_display = df.replace({True: '✅', False: '❌'})

    # Define output path to desktop
    desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
    output_filename = os.path.join(desktop_path, "crypto_exchange_support.xlsx")
    
    try:
        # Create a Pandas Excel writer using openpyxl as the engine.
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            # Write the DataFrame to an Excel sheet. index_label is for the first column (Cryptos)
            df_display.to_excel(writer, sheet_name='Crypto Support', index_label='Crypto')

            # Get the openpyxl workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Crypto Support']

            # Auto-adjust column widths
            for i, column in enumerate(worksheet.columns):
                max_length = 0
                # Get the column letter (e.g., 'A', 'B')
                # The first column is the index, so its header is at column[0]
                # For subsequent columns, the header is in the first row.
                column_header = worksheet.cell(row=1, column=i+1).value if i > 0 else 'Crypto' # Adjust for index header
                
                # Check length of column header
                if column_header:
                    max_length = len(str(column_header))

                for cell in column:
                    try: # Necessary to avoid error on empty cells
                        if cell.value is not None and len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2) * 1.2 # Add a little buffer
                worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
        
        logger.info(f"Excel report '{output_filename}' generated successfully at {os.path.abspath(output_filename)}")
    except Exception as e:
        logger.error(f"Failed to generate Excel report: {type(e).__name__} - {str(e)}")

if __name__ == "__main__":
    logger.info("Starting cryptocurrency exchange support cataloging...")
    
    support_data = get_exchange_crypto_support()
    
    if support_data:
        generate_excel_report(support_data)
    else:
        logger.error("No support data collected. Excel report not generated.")
    
    logger.info("Process finished.")