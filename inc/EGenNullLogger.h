// EGenNullLogger.h
//   2008 Yasufumi Kinoshita

#ifndef EGEN_NULL_LOGGER_H
#define EGEN_NULL_LOGGER_H

#include <sstream>
#include <fstream>

#include "EGenStandardTypes.h"
#include "DriverParamSettings.h"
#include "EGenVersion.h"
#include "BaseLogger.h"
#include "EGenLogFormatterTab.h"

namespace TPCE
{

class CEGenNullLogger : public CBaseLogger
{
private:
    bool SendToLoggerImpl(const char *szPrefix, const char *szTimestamp, const char *szMsg)
    {
        return true;
    }

public:
    CEGenNullLogger(eDriverType drvType, UINT32 UniqueId, const char *szFilename, CBaseLogFormatter* pLogFormatter)
    : CBaseLogger(drvType, UniqueId, pLogFormatter)
    {
    };
};

}   // namespace TPCE

#endif //EGEN_NULL_LOGGER_H
