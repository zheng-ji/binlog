#include "binlog.h"
#include <string.h>

#include <sys/stat.h>   
#include <unistd.h>

using namespace std;

BinLog::BinLog()
{
    m_iRWMode = 0;
    m_iBinLogId = 0;
    m_pRFile = NULL;
    m_pWFile = NULL;

    int ret = pthread_mutex_init(&mutex, NULL);  
    if (ret != 0)  
    {  
        perror("pthread_mutex_init failed\n");  
        exit(EXIT_FAILURE);  
    }  
}

BinLog::~BinLog() {} 

void BinLog::Close()
{
    //先关闭旧文件
    if (m_pWFile != NULL) fclose(m_pWFile);
    if (m_pRFile != NULL) fclose(m_pRFile);

    m_iBinLogId = 0;
    m_sFile = "";
}

int BinLog::Init(uint32_t iRWMode)
{
    m_iRWMode = iRWMode;
    time_t nowtime;
    nowtime = time(NULL); //获取日历时间
    char tmp[64];
    strftime(tmp,sizeof(tmp),"%Y%m%d",localtime(&nowtime));   
    
    uint32_t iDate = atoll(tmp);
    if (!SwitchBinLog( iDate )) return 1;

    mkdir(BINLOG_DIR, 0777);

    return 0;
}

bool BinLog::SwitchBinLog( uint32_t iBinLogId )
{
    if (iBinLogId == 0) return false;

    if (m_iBinLogId != iBinLogId)
    {
        pthread_mutex_lock(&mutex);  
        //加上线程锁 防止多线程并发的跳文件
        m_sFile = BINLOG_DIR + std::to_string(iBinLogId) + ".bin";
        bool bNew = false;
        struct stat buf;
        if (stat(m_sFile.c_str(), &buf) < 0)
        {
            bNew = true;
        }
        else if (buf.st_size == 0)
        {
            bNew = true;
        }

        if (bNew)
        {
            printf("%s gen new file %s",__func__, m_sFile.c_str());
        }

        //先关闭旧文件
        if (m_pWFile != NULL) 
        {
            fclose(m_pWFile);m_pWFile = NULL;
        }

        if (m_pRFile != NULL)
        {
            fclose(m_pRFile);m_pRFile = NULL;
        }

        //open file
        m_pWFile = fopen(m_sFile.c_str(),"ab+");
        if (m_pWFile == NULL)
        {
            printf("open fail %s\n",m_sFile.c_str());
            return false;
        }

        m_pRFile = fopen(m_sFile.c_str(),"rb");
        if (m_pRFile == NULL)
        {
            printf("open fail %s\n",m_sFile.c_str());
            return false;
        }

        m_iBinLogId = iBinLogId;
        printf("%s switch to new file %s\n",__func__, m_sFile.c_str());
        pthread_mutex_unlock(&mutex); 
    }
    return true;
}

int BinLog::WriteN(BinLogItem *ptBinLogItem, uint32_t iSize)
{
    if (m_pWFile == NULL)
    {
        printf("%s m_pWFile == NULL",__func__);
        return 0;
    }

    if (m_iRWMode != MODE_RW) 
    {
        printf(" read only \n");
        return 0;
    }

    int ret = fseek(m_pWFile, 0, SEEK_END);
    if (ret != 0)
    {
        printf("ERR:%s fseek end ret %d err %s",__func__, ret, strerror(errno));
        return 0;
    }

    uint32_t iWriteCnt = 0;
    uint64_t pos = ftell(m_pWFile);
    
    for (; iWriteCnt < iSize; iWriteCnt++)
    {
        BinLogItem &tItem = ptBinLogItem[iWriteCnt];
        tItem.iPos = pos;

        uint32_t &len = tItem.tKey.len;
        
        printf("write len: %u, buff: %s, iPos: %u\n", len, tItem.tKey.buff.c_str(), tItem.iPos);
        int ret;

        if (!Write((char *)&len, sizeof(uint32_t)))
        {
            break;
        }            
        
        if (!Write(&tItem.tKey.buff[0], len))
        {
            break;
        }

        pos += sizeof(uint32_t) + len;
    }

    if (iWriteCnt < iSize)
    {
        printf("%s fwrite need_size %u write_cnt %u", __func__, iSize, iWriteCnt);
    }

    return iWriteCnt;
}

int BinLog:: ReadN(uint64_t iPos, BinLogItem *ptBinLogItem, uint32_t iSize, uint64_t * piTailPos )
{
    if (m_pRFile == NULL) 
    {
        printf("%s m_pRFile == NULL",__func__);
        return 0;
    }

    int ret = fseek(m_pRFile, 0, SEEK_END);
    if (ret != 0)
    {
        printf("ERR:%s fseek ret %d err %s",__func__, ret, strerror(errno));
        return 0;
    }

    uint64_t iLen = ftell(m_pRFile);
    if (piTailPos) *piTailPos = iLen;

    //POS 非法
    if (iPos > iLen)
    {
        return ERR_POS_EXCEED;
    }
    //刚刚好到下一个写入位置，返回0
    else if (iPos == iLen)
    {
        return 0;
    }

    ret = fseek(m_pRFile, iPos, SEEK_SET);
    if (ret != 0)
    {
        printf("ERR:%s fseek ret %d err %s",__func__, ret, strerror(errno));
        return 0;
    }

    uint32_t iReadCnt = 0;
    for (; iReadCnt < iSize; iReadCnt++)
    {
        BinLogItem &tItem = ptBinLogItem[iReadCnt];
        tItem.iPos = ftell(m_pRFile);

        uint32_t &len = tItem.tKey.len;
        if (fread(&len, 1, sizeof(uint32_t), m_pRFile) != sizeof(uint32_t))
        {
            break;
        }

        tItem.tKey.buff.resize(len, 0);

        if (fread(&tItem.tKey.buff[0], 1, len, m_pRFile) != len)
        {
            break;
        }            
    }
    
    if (iReadCnt < iSize)
    {
        printf("%s fread pos %u need_size %u read_cnt %u  tell %u", __func__, iPos, iSize, iReadCnt, iLen);
    }

    return iReadCnt;
}

int BinLog::ReadFromHead()
{
    int ret = fseek(m_pRFile, 0, SEEK_SET);
    if (ret != 0)
    {
        printf("ERR:%s fseek ret %d err %s",__func__, ret, strerror(errno));
        return 0;
    }

    uint32_t iCnt = 0;

    while (true)
    {
        BinLogItem tItem;            
        tItem.iPos = ftell(m_pRFile);
        
        uint32_t &len = tItem.tKey.len;
        if (fread((char*)&len, 1, sizeof(uint32_t), m_pRFile) != sizeof(uint32_t))
        {
            break;
        }

        tItem.tKey.buff.resize(len, 0);            
        if (fread((char*)&tItem.tKey.buff[0], 1, len, m_pRFile) != len)
        {
            break;
        }

        printf("read binlog item %zu %s\n", tItem.iPos, tItem.ToString().c_str());
        iCnt++;            
    }
    
    return iCnt;
}

void BinLog::GrepItem(const string & sKey)
{
    fseek(m_pRFile, 0, SEEK_SET);

    const uint32_t iBatch = 1000;
    BinLogItem tItemList[iBatch];
    uint64_t iPos = 0;
    uint64_t iTailPos = 0;
    while (true)
    {
        uint32_t iReadCnt = ReadN(iPos, tItemList, iBatch, &iTailPos);
        if (iReadCnt == 0) break;

        for (int i=0; i < iReadCnt; i++)
        {
            if (tItemList[i].tKey.buff == sKey) 
            {
                printf("get %s\n", tItemList[i].tKey.buff.c_str());
                break;
            }
        }
        iPos = tItemList[iReadCnt-1].iPos + sizeof(tItemList[iReadCnt-1].tKey.len) + tItemList[iReadCnt-1].tKey.len;
    }
}

bool BinLog::Write(const char *buf, uint32_t write_len) 
{

    uint32_t write_count = 0;

    while (true) 
    {
        size_t ret = fwrite(buf + write_count, 1, write_len - write_count, m_pWFile);
        write_count += ret;

        if (write_count == write_len) 
        {
            break;
        }

        if (write_count != write_len && ret == 0) 
        {
            printf("ERR:%s write log fail, write_count %u != write_len %u", __func__, write_count, write_len);
            fprintf(stderr, "write log failed.\n");
            return false;
        }
    }
    fflush(m_pWFile);

    return true;
}

bool BinLog::Read(char *buf, uint32_t read_len) 
{
    uint32_t read_count = 0;

    while (true) 
    {
        size_t ret = fread(buf + read_count, 1, read_len - read_count, m_pRFile);
        read_count += ret;

        if (read_count == read_len) 
        {
            break;
        }

        if (read_count != read_len && ret == 0)
        {
            printf("%s read log failed len %u read_count %u ret %zu err %s\n", __func__, read_len, read_count,ret, strerror(ferror(m_pRFile)));
            return false;
        }
    }

    return true;
}
