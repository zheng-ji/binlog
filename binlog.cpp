#include "binlog.h"
#include <string.h>

#include <sys/stat.h>   
#include <unistd.h>

using namespace std;

namespace bfbinlog{
    
    BinLog::BinLog()
    {
        m_iRWMode = 0;
        m_iBinLogId = 0;
        m_pRFile = NULL;
        m_pWFile = NULL;

        int ret = pthread_mutex_init(&mutex, NULL);  
        if(ret != 0)  
        {  
            perror("pthread_mutex_init failed\n");  
            exit(EXIT_FAILURE);  
        }  
    }

    BinLog::~BinLog()
    {
        //fclose(m_pRFile);
        //fclose(m_pWFile);
    }

    void BinLog::Close()
    {
        //先关闭旧文件
        if( m_pWFile != NULL ) fclose(m_pWFile);
        if( m_pRFile != NULL ) fclose(m_pRFile);

        m_iBinLogId = 0;
        m_sFile = "";
    }

    int BinLog::Init( uint32_t iRWMode )
    {
        m_iRWMode = iRWMode;


        time_t nowtime;
        nowtime = time(NULL); //获取日历时间
        char tmp[64];
        strftime(tmp,sizeof(tmp),"%Y%m%d",localtime(&nowtime));   
        
        uint32_t iDate = atoll(tmp);
        if ( !SwitchBinLog( iDate ) ) return 1;

        //create dir BINLOG_DIR
        //Comm::PrepareFolder(BINLOG_DIR);
        //TODO
        mkdir(BINLOG_DIR, 0777);

        return 0;
    }

    bool BinLog::SwitchBinLog( uint32_t iBinLogId )
    {
        if( iBinLogId == 0 ) return false;

        if( m_iBinLogId != iBinLogId )
        {
            pthread_mutex_lock(&mutex);  
            //加上线程锁 防止多线程并发的跳文件
            //m_sFile = Comm::StrFormat("%s%u.bin",BINLOG_DIR, iBinLogId);
            m_sFile = BINLOG_DIR + std::to_string(iBinLogId) + ".bin";
            bool bNew = false;
            struct stat buf;
            if( stat(m_sFile.c_str(), &buf) < 0 )
            {
                bNew = true;
            }
            else if( buf.st_size == 0 )
            {
                bNew = true;
            }

            if( bNew )
            {
                printf("%s gen new file %s",__func__, m_sFile.c_str());
            }

            //printf("debug start switch to new file %s m_iBinLogId %u iBinLogId %u m_pWFile %p m_pRFile %p \n",m_sFile.c_str(), m_iBinLogId, iBinLogId, m_pWFile, m_pRFile );

            //先关闭旧文件
            if( m_pWFile != NULL ) 
            {
                fclose(m_pWFile);m_pWFile = NULL;
            }
            if( m_pRFile != NULL )
            {
                fclose(m_pRFile);m_pRFile = NULL;
            }

            //open file
            m_pWFile = fopen(m_sFile.c_str(),"ab+");
            if(m_pWFile == NULL)
            {
                printf("open fail %s\n",m_sFile.c_str());
                return false;
            }

            m_pRFile = fopen(m_sFile.c_str(),"rb");
            if(m_pRFile == NULL)
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

    int BinLog::WriteN( BinLogItem *ptBinLogItem, uint32_t iSize )
    {
        if( m_pWFile == NULL)
        {
            printf("%s m_pWFile == NULL",__func__);
            return 0;
        }

        if( m_iRWMode != MODE_RW ) 
        {
            printf(" read only \n");
            return 0;
        }


        int ret = fseek(m_pWFile, 0, SEEK_END);
        if( ret != 0 )
        {
            printf("ERR:%s fseek end ret %d err %s",__func__, ret, strerror(errno));
            return 0;
        }

        uint64_t iPos = ftell(m_pWFile);
        uint64_t iDelta = 0;

        if (iPos % sizeof(BinLogItem) != 0) {
            iDelta = sizeof(BinLogItem) - iPos % sizeof(BinLogItem);
            BinLogItem tmpItem;
            memset(&tmpItem, 0x0, sizeof(BinLogItem));
            Write((char*)(&tmpItem), iDelta);
        }

        for(int i=0; i<iSize; i++)
        {
            ptBinLogItem[i].iPos = iPos + iDelta + i*sizeof(BinLogItem);
        }

        if( !Write((char*)ptBinLogItem, sizeof(BinLogItem)*iSize))
        {
            return 0;
        }

        return iSize;
    }

    int BinLog:: ReadN( uint64_t iPos, BinLogItem *ptBinLogItem, uint32_t iSize, uint64_t * piTailPos )
    {
        if( m_pRFile == NULL ) 
        {
            printf("%s m_pRFile == NULL",__func__);
            return 0;
        }

        int ret = fseek(m_pRFile, 0, SEEK_END);
        if( ret != 0 )
        {
            printf("ERR:%s fseek ret %d err %s",__func__, ret, strerror(errno));
            return 0;
        }

        uint64_t iLen = ftell(m_pRFile);
        if(piTailPos) *piTailPos = iLen;

        //POS 非法
        if( iPos > iLen )
        {
            return ERR_POS_EXCEED;
        }
        //刚刚好到下一个写入位置，返回0
        else if ( iPos == iLen )
        {
            return 0;
        }

        //uint64_t iReadLen = iPos + sizeof(BinLogItem)*iSize > iLen ? iLen - iPos : sizeof(BinLogItem)*iSize;

        ret = fseek(m_pRFile, iPos, SEEK_SET);
        if( ret != 0 )
        {
            printf("ERR:%s fseek ret %d err %s",__func__, ret, strerror(errno));
            return 0;
        }

        int iReadCnt = fread((char*)ptBinLogItem, sizeof(BinLogItem), iSize, m_pRFile);
        if( iReadCnt < iSize )
        {
            printf("%s fread pos %u need_size %u read_cnt %u  tell %u", __func__, iPos, iSize, iReadCnt, iLen);
        }

        return iReadCnt;
    }

    int BinLog::ReadFromHead( )
    {
        int ret = fseek(m_pRFile, 0, SEEK_SET);
        if( ret != 0 )
        {
            printf("ERR:%s fseek ret %d err %s",__func__, ret, strerror(errno));
            return 0;
        }

        BinLogItem tItem;
        uint32_t iCnt = 0;
        while( fread((char*)&tItem, 1, sizeof(BinLogItem), m_pRFile) == sizeof(BinLogItem) )
        {
            printf("read binlog item %zu %s\n", 
                   iCnt * sizeof(BinLogItem), tItem.ToString().c_str());
            iCnt++;
        }

        return iCnt;

    }

    void BinLog::GrepAppMsg( uint32_t iExpId, uint32_t iMetricsId)
    {
        fseek(m_pRFile, 0, SEEK_SET);

        const uint32_t iBatch = 1000;
        BinLogItem tItemList[iBatch];
        while(true)
        {
            size_t ret = fread((char*)&tItemList, 1, iBatch *sizeof(BinLogItem), m_pRFile);
            if( ret == 0 ) break;
            
            uint32_t iReadCnt = ret/sizeof(BinLogItem);

            for( int i=0;i<iReadCnt;i++)
            {
                if( tItemList[i].tKey.iExpId== iExpId && tItemList[i].tKey.iMetricsId == iMetricsId)
                {
                    printf("%s\n", tItemList[i].ToString().c_str());
                }
            }
        }
    }

    bool BinLog::Write(const char *buf, uint32_t write_len) {

        uint32_t write_count = 0;

        while (true) {
            size_t ret = fwrite(buf + write_count, 1, write_len - write_count, m_pWFile);
            write_count += ret;

            if (write_count == write_len) {
                break;
            }

            if (write_count != write_len && ret == 0) {
                printf("ERR:%s write log fail, write_count %u != write_len %u", __func__, write_count, write_len);
                fprintf(stderr, "write log failed.\n");
                return false;
            }
        }
        fflush(m_pWFile);

        return true;
    }

    bool BinLog::Read(char *buf, uint32_t read_len) {
        uint32_t read_count = 0;

        while (true) {
            size_t ret = fread(buf + read_count, 1, read_len - read_count, m_pRFile);
            read_count += ret;

            if (read_count == read_len) {
                break;
            }

            if( read_count != read_len && ret == 0 )
            {
                printf("%s read log failed len %u read_count %u ret %zu err %s\n", __func__, read_len, read_count,ret, strerror(ferror(m_pRFile)));
                return false;
            }
        }

        return true;
    }
}

