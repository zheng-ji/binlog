#pragma once 

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string>

#include "pthread.h"

#define BINLOG_DIR "/tmp/" 

#define BINLOG_VERSION 1
#define BINLOG_KEEP_DAy  7

using namespace std;

#pragma pack(push) 
#pragma pack(1)

typedef struct _Key
{
    uint32_t     len;
    std::string  buff;

    string ToString()
    {
        return buff;
    }

}Key;

typedef struct _BinLogItem
{
    uint64_t iPos;
    Key tKey;

    string ToString()
    {
        return "pos " + std::to_string(iPos) + " buff " + tKey.buff;
    }

}BinLogItem;

enum RWMODE
{
    MODE_RONLY = 0,
    MODE_RW = 1,
};

enum ERR_CODE
{
    ERR_POS_EXCEED = -1, 
    ERR_POS_NOT_RIGHT = -2,
};

#pragma pack(pop) 

class BinLog {

public :
    BinLog( );
    ~BinLog();

    int Init( uint32_t iRWMode = 0 );

    void Close();

    int ReadN( uint64_t iPos, BinLogItem *ptBinLogItem, uint32_t iSize, uint64_t * piTailPos = NULL );

    int WriteN( BinLogItem *ptBinLogItem, uint32_t iSize );

    bool SwitchBinLog( uint32_t iBinLogId );

    int ReadFromHead();

    void GrepItem(const string& sKey);

    bool Write(const char *buf, uint32_t write_len);
    bool Read(char *buf, uint32_t read_len);

    uint32_t GetBinLogId(){ return m_iBinLogId; }

private:

    std::string GetCurBinLog();

    FILE * m_pRFile;
    FILE * m_pWFile;

    uint32_t m_iBinLogId;
    std::string m_sFile;
    std::string m_sDir;

    int m_iRWMode;
    pthread_mutex_t mutex;  
};

