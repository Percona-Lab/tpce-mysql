// SendToMarket.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"

using namespace TPCE;

void push_message(PTradeRequest pMessage);

bool CSendToMarket::SendToMarket(TTradeRequest &trade_mes)
{
    push_message(&trade_mes);
    return true;
}
