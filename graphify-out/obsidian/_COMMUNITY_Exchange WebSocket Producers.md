---
type: community
cohesion: 0.03
members: 108
---

# Exchange WebSocket Producers

**Cohesion:** 0.03 - loosely connected
**Members:** 108 nodes

## Members
- [[.__init__()_5]] - code - src/ingestion/base_producer.py
- [[.__init__()_8]] - code - src/ingestion/binance_producer.py
- [[.__init__()_7]] - code - src/ingestion/coinbase_producer.py
- [[.__init__()_14]] - code - src/utils/kafka_utils.py
- [[.__init__()_6]] - code - src/ingestion/kraken_producer.py
- [[._on_send_error()]] - code - src/utils/kafka_utils.py
- [[._on_send_success()]] - code - src/utils/kafka_utils.py
- [[._parse_orderbook()_2]] - code - src/ingestion/binance_producer.py
- [[._parse_orderbook()_1]] - code - src/ingestion/coinbase_producer.py
- [[._parse_orderbook()]] - code - src/ingestion/kraken_producer.py
- [[._parse_spread()]] - code - src/ingestion/kraken_producer.py
- [[._parse_ticker()_2]] - code - src/ingestion/binance_producer.py
- [[._parse_ticker()_1]] - code - src/ingestion/coinbase_producer.py
- [[._parse_ticker()]] - code - src/ingestion/kraken_producer.py
- [[._parse_trade()_2]] - code - src/ingestion/binance_producer.py
- [[._parse_trade()_1]] - code - src/ingestion/coinbase_producer.py
- [[._parse_trade()]] - code - src/ingestion/kraken_producer.py
- [[.flush()]] - code - src/utils/kafka_utils.py
- [[.get_kafka_topic()_2]] - code - src/ingestion/binance_producer.py
- [[.get_kafka_topic()_1]] - code - src/ingestion/coinbase_producer.py
- [[.get_kafka_topic()]] - code - src/ingestion/kraken_producer.py
- [[.get_subscribe_message()_2]] - code - src/ingestion/binance_producer.py
- [[.get_subscribe_message()_1]] - code - src/ingestion/coinbase_producer.py
- [[.get_subscribe_message()]] - code - src/ingestion/kraken_producer.py
- [[.get_websocket_url()_2]] - code - src/ingestion/binance_producer.py
- [[.get_websocket_url()_1]] - code - src/ingestion/coinbase_producer.py
- [[.get_websocket_url()]] - code - src/ingestion/kraken_producer.py
- [[.on_close()]] - code - src/ingestion/base_producer.py
- [[.on_error()]] - code - src/ingestion/base_producer.py
- [[.on_message()]] - code - src/ingestion/base_producer.py
- [[.on_open()]] - code - src/ingestion/base_producer.py
- [[.parse_message()_2]] - code - src/ingestion/binance_producer.py
- [[.parse_message()_1]] - code - src/ingestion/coinbase_producer.py
- [[.parse_message()]] - code - src/ingestion/kraken_producer.py
- [[.send()]] - code - src/utils/kafka_utils.py
- [[.start()]] - code - src/ingestion/base_producer.py
- [[.stop()]] - code - src/ingestion/base_producer.py
- [[ABC]] - code
- [[Abstract base class for exchange WebSocket producers.]] - rationale - src/ingestion/base_producer.py
- [[Base WebSocket producer for crypto exchanges.]] - rationale - src/ingestion/base_producer.py
- [[BaseProducer]] - code - src/ingestion/base_producer.py
- [[BaseProducer_1]] - code
- [[Binance WebSocket producer for market data.]] - rationale - src/ingestion/binance_producer.py
- [[Binance WebSocket producer.]] - rationale - src/ingestion/binance_producer.py
- [[Binance doesn't require subscription message for combined streams.          Retu]] - rationale - src/ingestion/binance_producer.py
- [[BinanceProducer]] - code - src/ingestion/binance_producer.py
- [[Callback for failed message delivery.]] - rationale - src/utils/kafka_utils.py
- [[Callback for successful message delivery.]] - rationale - src/utils/kafka_utils.py
- [[Callback when WebSocket connection is closed.          Args             ws Web]] - rationale - src/ingestion/base_producer.py
- [[Callback when WebSocket connection is opened.]] - rationale - src/ingestion/base_producer.py
- [[Callback when WebSocket error occurs.          Args             ws WebSocket i]] - rationale - src/ingestion/base_producer.py
- [[Callback when WebSocket message is received.          Args             ws WebS]] - rationale - src/ingestion/base_producer.py
- [[Coinbase Pro WebSocket producer for market data.]] - rationale - src/ingestion/coinbase_producer.py
- [[Coinbase Pro WebSocket producer.]] - rationale - src/ingestion/coinbase_producer.py
- [[CoinbaseProducer]] - code - src/ingestion/coinbase_producer.py
- [[Flush pending messages.]] - rationale - src/utils/kafka_utils.py
- [[Get Coinbase Pro subscription message.          Returns             Subscriptio]] - rationale - src/ingestion/coinbase_producer.py
- [[Get Kafka topic based on message type.          Args             message_type]] - rationale - src/ingestion/base_producer.py
- [[Get Kafka topic based on message type.          Args             message_type_3]] - rationale - src/ingestion/binance_producer.py
- [[Get Kafka topic based on message type.          Args             message_type_2]] - rationale - src/ingestion/coinbase_producer.py
- [[Get Kafka topic based on message type.          Args             message_type_1]] - rationale - src/ingestion/kraken_producer.py
- [[Get Kraken subscription message.          Returns             Subscription mess]] - rationale - src/ingestion/kraken_producer.py
- [[Get WebSocket URL for Binance combined streams.          Returns             We]] - rationale - src/ingestion/binance_producer.py
- [[Get WebSocket URL for Coinbase Pro.          Returns             WebSocket URL]] - rationale - src/ingestion/coinbase_producer.py
- [[Get WebSocket URL for Kraken.          Returns             WebSocket URL]] - rationale - src/ingestion/kraken_producer.py
- [[Get WebSocket URL for the exchange.          Returns             WebSocket URL]] - rationale - src/ingestion/base_producer.py
- [[Get subscription message to send after connection.          Returns]] - rationale - src/ingestion/base_producer.py
- [[Initialize Binance producer.          Args             kafka_bootstrap_servers]] - rationale - src/ingestion/binance_producer.py
- [[Initialize Coinbase producer.          Args             kafka_bootstrap_servers]] - rationale - src/ingestion/coinbase_producer.py
- [[Initialize Kafka producer.          Args             bootstrap_servers Kafka b]] - rationale - src/utils/kafka_utils.py
- [[Initialize Kraken producer.          Args             kafka_bootstrap_servers]] - rationale - src/ingestion/kraken_producer.py
- [[Initialize base producer.          Args             exchange_name Name of the]] - rationale - src/ingestion/base_producer.py
- [[KafkaProducerWrapper]] - code - src/utils/kafka_utils.py
- [[Kraken WebSocket producer for market data.]] - rationale - src/ingestion/kraken_producer.py
- [[Kraken WebSocket producer.]] - rationale - src/ingestion/kraken_producer.py
- [[KrakenProducer]] - code - src/ingestion/kraken_producer.py
- [[Main entry point for Binance producer.]] - rationale - src/ingestion/binance_producer.py
- [[Main entry point for Coinbase producer.]] - rationale - src/ingestion/coinbase_producer.py
- [[Main entry point for Kraken producer.]] - rationale - src/ingestion/kraken_producer.py
- [[Parse Binance WebSocket message.          Args             message Raw WebSock]] - rationale - src/ingestion/binance_producer.py
- [[Parse Coinbase Pro WebSocket message.          Args             message Raw We]] - rationale - src/ingestion/coinbase_producer.py
- [[Parse Kraken WebSocket message.          Args             message Raw WebSocke]] - rationale - src/ingestion/kraken_producer.py
- [[Parse incoming WebSocket message.          Args             message Raw WebSoc]] - rationale - src/ingestion/base_producer.py
- [[Parse order book message.          Args             data Order book data_2]] - rationale - src/ingestion/binance_producer.py
- [[Parse order book message.          Args             data Order book data_1]] - rationale - src/ingestion/coinbase_producer.py
- [[Parse order book message.          Args             data Order book data]] - rationale - src/ingestion/kraken_producer.py
- [[Parse spread message.          Args             data Spread data             p]] - rationale - src/ingestion/kraken_producer.py
- [[Parse ticker message.          Args             data Ticker data             p]] - rationale - src/ingestion/kraken_producer.py
- [[Parse ticker message.          Args             data Ticker data             s]] - rationale - src/ingestion/binance_producer.py
- [[Parse ticker message.          Args             data Ticker data          Retu]] - rationale - src/ingestion/coinbase_producer.py
- [[Parse trade message.          Args             data Trade data             str]] - rationale - src/ingestion/binance_producer.py
- [[Parse trade message.          Args             data Trade data          Return]] - rationale - src/ingestion/coinbase_producer.py
- [[Parse trade message.          Args             data Trade data array]] - rationale - src/ingestion/kraken_producer.py
- [[Send message to Kafka topic.          Args             topic Kafka topic name]] - rationale - src/utils/kafka_utils.py
- [[Start the WebSocket connection.]] - rationale - src/ingestion/base_producer.py
- [[Stop the WebSocket connection.]] - rationale - src/ingestion/base_producer.py
- [[Wrapper for Kafka producer with built-in error handling and serialization.]] - rationale - src/utils/kafka_utils.py
- [[base_producer.py]] - code - src/ingestion/base_producer.py
- [[binance_producer.py]] - code - src/ingestion/binance_producer.py
- [[coinbase_producer.py]] - code - src/ingestion/coinbase_producer.py
- [[get_kafka_topic()]] - code - src/ingestion/base_producer.py
- [[get_subscribe_message()]] - code - src/ingestion/base_producer.py
- [[get_websocket_url()]] - code - src/ingestion/base_producer.py
- [[kraken_producer.py]] - code - src/ingestion/kraken_producer.py
- [[main()_2]] - code - src/ingestion/binance_producer.py
- [[main()_1]] - code - src/ingestion/coinbase_producer.py
- [[main()]] - code - src/ingestion/kraken_producer.py
- [[parse_message()]] - code - src/ingestion/base_producer.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Exchange_WebSocket_Producers
SORT file.name ASC
```

## Connections to other communities
- 5 edges to [[_COMMUNITY_Data Cache & Storage Layer]]
- 4 edges to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 2 edges to [[_COMMUNITY_Kafka Utilities]]

## Top bridge nodes
- [[KafkaProducerWrapper]] - degree 23, connects to 2 communities
- [[BaseProducer]] - degree 49, connects to 1 community
- [[KrakenProducer]] - degree 15, connects to 1 community
- [[BinanceProducer]] - degree 14, connects to 1 community
- [[CoinbaseProducer]] - degree 14, connects to 1 community