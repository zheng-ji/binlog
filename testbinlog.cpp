#include "binlog.h"
#include <string.h>

using namespace std;

void ShowUsage( const char * pcProgram )
{
    printf("Usage:%s readn pos cnt [binlogid]\n",pcProgram);
    printf("%s writen cnt string\n",pcProgram);
    printf("%s readall [binlogid]\n",pcProgram);
    printf("%s grep string [binlogid]\n",pcProgram);
}

int main( int argc, char ** argv )
{
    BinLog binlog;
    binlog.Init(MODE_RW);

    if (argc < 2)
    {
        ShowUsage( argv[0]);
        return -1;
    }

    char * pcFunc = argv[1];

    if ( (strcasecmp(pcFunc, "readall") == 0) && (argc >= 2) )
    {
        if( argc >= 3 )
        {
            uint32_t iBinLogId = atoll(argv[2]);
            binlog.SwitchBinLog(iBinLogId);
        }
        printf("ReadFromHead------------\n");
        uint32_t iCnt = binlog.ReadFromHead();
        printf("ReadFromHead End %d\n\n", iCnt);

    }
    else if ( (strcasecmp(pcFunc, "readn") == 0) && (argc >= 4) )
    {
        uint64_t iPos = atoll(argv[2]);
        uint32_t N = atoll(argv[3]);
        if (argc >= 5)
        {
            uint32_t iBinLogId = atoll(argv[4]);
            binlog.SwitchBinLog(iBinLogId);
        }
        printf("ReadN------------\n");
        BinLogItem ptList[N];
        uint64_t iTailPos = 0;
        uint32_t iCnt = binlog.ReadN(iPos, ptList, N, &iTailPos);
        for (int i = 0; i<iCnt; i++)
        {
            printf("%s\n", ptList[i].ToString().c_str());
        }
        printf("ReadN End %d tail pos %u\n\n", iCnt, iTailPos);
        
    }
    else if (strcasecmp(pcFunc, "writen") == 0)
    {
        uint32_t N = atoll(argv[2]);
        printf("WriteN------------\n");

        BinLogItem ptList[N];
        for (int i = 0; i < N; i++)
        {
            ptList[i].iPos = 0;
            ptList[i].tKey.buff = std::string(argv[3]) + std::to_string(i);
            ptList[i].tKey.len = ptList[i].tKey.buff.size();
        }

        uint32_t iCnt = binlog.WriteN(ptList, N);
        printf("WriteN End %d------------\n\n", iCnt);
    }
    else if (strcasecmp(pcFunc, "grep") == 0)
    {
        uint32_t iBinLogId = atoll(argv[3]);
        binlog.SwitchBinLog(iBinLogId);
        string grepString = argv[2];
        printf("%s, %u", grepString.c_str(), iBinLogId);
        binlog.GrepItem(grepString);
    }
    return 0;
}
