#ifndef INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_GATEIO_H_
#define INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_GATEIO_H_
#ifdef CCAPI_ENABLE_SERVICE_MARKET_DATA
#ifdef CCAPI_ENABLE_EXCHANGE_GATEIO
#include "ccapi_cpp/service/ccapi_market_data_service_gateio_base.h"
namespace ccapi {
class MarketDataServiceGateio : public MarketDataServiceGateioBase {
 public:
#if defined ENABLE_EPOLL_HTTPS_CLIENT || defined ENABLE_EPOLL_WS_CLIENT
  MarketDataServiceGateio(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                          ServiceContext* serviceContextPtr, emumba::connector::io_handler& io)
      : MarketDataServiceGateioBase(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr, io) {
#else
  MarketDataServiceGateio(std::function<void(Event&, Queue<Event>*)> eventHandler, SessionOptions sessionOptions, SessionConfigs sessionConfigs,
                          std::shared_ptr<ServiceContext> serviceContextPtr)
      : MarketDataServiceGateioBase(eventHandler, sessionOptions, sessionConfigs, serviceContextPtr) {
#endif
    this->exchangeName = CCAPI_EXCHANGE_NAME_GATEIO;
    this->baseUrlWs = sessionConfigs.getUrlWebsocketBase().at(this->exchangeName) + "/ws/v4/";
    this->baseUrlRest = sessionConfigs.getUrlRestBase().at(this->exchangeName);
    this->setHostRestFromUrlRest(this->baseUrlRest);
    this->setHostWsFromUrlWs(this->baseUrlWs);
    //     try {
    //       this->tcpResolverResultsRest = this->resolver.resolve(this->hostRest, this->portRest);
    //     } catch (const std::exception& e) {
    //       CCAPI_LOGGER_FATAL(std::string("e.what() = ") + e.what());
    //     }
    // #ifdef CCAPI_LEGACY_USE_WEBSOCKETPP
    // #else
    //     try {
    //       this->tcpResolverResultsWs = this->resolverWs.resolve(this->hostWs, this->portWs);
    //     } catch (const std::exception& e) {
    //       CCAPI_LOGGER_FATAL(std::string("e.what() = ") + e.what());
    //     }
    // #endif
    this->apiKeyName = CCAPI_GATEIO_API_KEY;
    this->setupCredential({this->apiKeyName});
    std::string prefix = "/api/v4";
    this->getRecentTradesTarget = prefix + "/spot/trades";
    this->getInstrumentTarget = prefix + "/spot/currency_pairs/{currency_pair}";
    this->getInstrumentsTarget = prefix + "/spot/currency_pairs";
    this->websocketChannelTrades = CCAPI_WEBSOCKET_GATEIO_CHANNEL_TRADES;
    this->websocketChannelBookTicker = CCAPI_WEBSOCKET_GATEIO_CHANNEL_BOOK_TICKER;
    this->websocketChannelOrderBook = CCAPI_WEBSOCKET_GATEIO_CHANNEL_ORDER_BOOK;
    this->websocketChannelCandlesticks = CCAPI_WEBSOCKET_GATEIO_CHANNEL_CANDLESTICKS;
    this->symbolName = "currency_pair";
  }
} void extractInstrumentInfo(Element& element, const rj::Value& x) {
  element.insert(CCAPI_INSTRUMENT, x["id"].GetString());
  element.insert(CCAPI_BASE_ASSET, x["base"].GetString());
  element.insert(CCAPI_QUOTE_ASSET, x["quote"].GetString());
  int precision = std::stoi(x["precision"].GetString());
  if (precision > 0) {
    element.insert(CCAPI_ORDER_PRICE_INCREMENT, "0." + std::string(precision - 1, '0') + "1");
  } else {
    element.insert(CCAPI_ORDER_PRICE_INCREMENT, "1");
  }
  int amountPrecision = std::stoi(x["amount_precision"].GetString());
  if (amountPrecision > 0) {
    element.insert(CCAPI_ORDER_QUANTITY_INCREMENT, "0." + std::string(amountPrecision - 1, '0') + "1");
  } else {
    element.insert(CCAPI_ORDER_QUANTITY_INCREMENT, "1");
  }
}
void convertTextMessageToMarketDataMessage(const Request& request, const std::string& textMessage, const TimePoint& timeReceived, Event& event,
                                           std::vector<MarketDataMessage>& marketDataMessageList) override {
  rj::Document document;
  document.Parse<rj::kParseNumbersAsStringsFlag>(textMessage.c_str());
  switch (request.getOperation()) {
    case Request::Operation::GET_RECENT_TRADES: {
      for (const auto& x : document.GetArray()) {
        MarketDataMessage marketDataMessage;
        marketDataMessage.type = MarketDataMessage::Type::MARKET_DATA_EVENTS_TRADE;
        marketDataMessage.tp = UtilTime::makeTimePointMilli(UtilTime::divideMilli(x["create_time_ms"].GetString()));
        MarketDataMessage::TypeForDataPoint dataPoint;
        dataPoint.insert({MarketDataMessage::DataFieldType::PRICE, UtilString::normalizeDecimalString(std::string(x["price"].GetString()))});
        dataPoint.insert({MarketDataMessage::DataFieldType::SIZE, UtilString::normalizeDecimalString(std::string(x["amount"].GetString()))});
        dataPoint.insert({MarketDataMessage::DataFieldType::TRADE_ID, std::string(x["id"].GetString())});
        dataPoint.insert({MarketDataMessage::DataFieldType::IS_BUYER_MAKER, std::string(x["side"].GetString()) == "buy" ? "1" : "0"});
        marketDataMessage.data[MarketDataMessage::DataType::TRADE].emplace_back(std::move(dataPoint));
        marketDataMessageList.emplace_back(std::move(marketDataMessage));
      }
    } break;
    case Request::Operation::GET_INSTRUMENT: {
      Message message;
      message.setTimeReceived(timeReceived);
      message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
      Element element;
      this->extractInstrumentInfo(element, document);
      message.setElementList({element});
      message.setCorrelationIdList({request.getCorrelationId()});
      event.addMessages({message});
    } break;
    case Request::Operation::GET_INSTRUMENTS: {
      Message message;
      message.setTimeReceived(timeReceived);
      message.setType(this->requestOperationToMessageTypeMap.at(request.getOperation()));
      std::vector<Element> elementList;
      for (const auto& x : document.GetArray()) {
        Element element;
        this->extractInstrumentInfo(element, x);
        elementList.push_back(element);
      }
      message.setElementList(elementList);
      message.setCorrelationIdList({request.getCorrelationId()});
      event.addMessages({message});
    } break;
    default:
      CCAPI_LOGGER_FATAL(CCAPI_UNSUPPORTED_VALUE);
  }
}
};  // namespace ccapi
} /* namespace ccapi */
#endif
#endif
#endif  // INCLUDE_CCAPI_CPP_SERVICE_CCAPI_MARKET_DATA_SERVICE_GATEIO_H_
