One data server with top 50 binance coins

52.194.229.217

binance-transactions_1

```
docker run --name binance-transactions-1 -d l0rtk/bitpulse_binance_transactions:0.3.1 python /app/src/get_binance_transactions.py db_name=bitpulse_v2 stats_collection=transactions_stats_second big_transactions_collection=big_transactions pairs=BTCUSDT,DOGEUSDT,ETHUSDT,PEPEUSDT,PNUTUSDT,USDCUSDT,SOLUSDT,FDUSDUSDT,XRPUSDT,ACTUSDT,SHIBUSDT,WIFUSDT,SUIUSDT,BONKUSDT,BNBUSDT,NEIROUSDT,FLOKIUSDT,ADAUSDT,BOMEUSDT,AVAXUSDT,FETUSDT,WLDUSDT,TRXUSDT,APTUSDT,NEARUSDT,RUNEUSDT,HBARUSDT,ENAUSDT,FTMUSDT,SEIUSDT,PEOPLEUSDT,RENDERUSDT,ORDIUSDT,ARBUSDT,TAOUSDT,LINKUSDT,DOTUSDT,MEMEUSDT,OPUSDT,RAYUSDT,TURBOUSDT,LTCUSDT,INJUSDT,TIAUSDT,TONUSDT,POLUSDT
```

binance-transactions_2

```
docker run --name binance-transactions-2 -d l0rtk/bitpulse_binance_transactions:0.3.1 python /app/src/get_binance_transactions.py db_name=bitpulse_v2 stats_collection=transactions_stats_second big_transactions_collection=big_transactions pairs=UNIUSDT,BCHUSDT,DOGSUSDT,NOTUSDT,XLMUSDT,AAVEUSDT,GALAUSDT,FILUSDT,ARKMUSDT,PENDLEUSDT,ARUSDT,JUPUSDT,PYTHUSDT,ICPUSDT,STXUSDT,CRVUSDT,APEUSDT,JTOUSDT,MKRUSDT,EIGENUSDT,ATOMUSDT,ETHFIUSDT,ETCUSDT,STRKUSDT,OMUSDT,JASMYUSDT,ZROUSDT,ZKUSDT,ENSUSDT,LDOUSDT,WBTCUSDT,LUNCUSDT,GRTUSDT,DYDXUSDT,CHZUSDT,WUSDT
```

kucoin-transactions_1

```
docker run --name kucoin-transactions-1 -d l0rtk/bitpulse_kucoin_transactions:1.0 python /app/src/get_kucoin_transactions.py db_name=bitpulse_v2 stats_collection=transactions_stats_second big_transactions_collection=big_transactions pairs=BTC-USDT,DOGE-USDT,ETH-USDT,PEPE-USDT,PNUT-USDT,USDC-USDT,SOL-USDT,XRP-USDT,SHIB-USDT,WIF-USDT,SUI-USDT,BONK-USDT,BNB-USDT,FLOKI-USDT,ADA-USDT,BOME-USDT,AVAX-USDT,FET-USDT,WLD-USDT,TRX-USDT,APT-USDT,NEAR-USDT,RUNE-USDT,HBAR-USDT,ENA-USDT,FTM-USDT
```

kucoin-transactions_2

```
docker run --name kucoin-transactions-2 -d l0rtk/bitpulse_kucoin_transactions:1.0 python /app/src/get_kucoin_transactions.py db_name=bitpulse_v2 stats_collection=transactions_stats_second big_transactions_collection=big_transactions pairs=SEI-USDT,PEOPLE-USDT,RENDER-USDT,ORDI-USDT,ARB-USDT,TAO-USDT,LINK-USDT,DOT-USDT,MEME-USDT,OP-USDT,RAY-USDT,TURBO-USDT,LTC-USDT,INJ-USDT,TIA-USDT,TON-USDT,POL-USDT,UNI-USDT,BCH-USDT,DOGS-USDT,NOT-USDT,XLM-USDT,AAVE-USDT,GALA-USDT
```
