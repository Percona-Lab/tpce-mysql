// SendToMarket.h
//   2008 Yasufumi Kinoshita

#ifndef SEND_TO_MARKET_H
#define SEND_TO_MARKET_H

namespace TPCE
{

class CSendToMarket : public CSendToMarketInterface
{
 public:
    CSendToMarket() {};
    ~CSendToMarket() {};

    virtual bool SendToMarket(TTradeRequest &trade_mes);
};

}   // namespace TPCE

#endif //SEND_TO_MARKET_H
