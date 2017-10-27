// RtHistogram.h
//   2008 Yasufumi Kinoshita

#ifndef RT_HISTOGRAM_H
#define RT_HISTOGRAM_H

#define RTHIST_MAX_MSEC 6000
#define RTHIST_CLASS_MSEC 10

namespace TPCE
{

class CRtHistogram
{
 protected:
    long m_Total_Hist[RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC];
    long m_Current_Hist[RTHIST_MAX_MSEC / RTHIST_CLASS_MSEC];
    CMutex m_RtHistLock;
    long clk_tck;

 public:
    long m_Total_Max;  // msec.
    long m_Current_Max;// msec.

    CRtHistogram();
    ~CRtHistogram();

    void Put(long rtclk);
    long Check_point(); //return 90%ile line
    void Print();
};

}   // namespace TPCE

#endif // RT_HISTOGRAM_H
