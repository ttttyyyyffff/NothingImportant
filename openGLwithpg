import requests
import random
import sys,re,os,time,socket,traceback,pymysql,json
from multiprocessing import process,Pool

colum_rank = '20190823'
colum_url = 'url%s'%colum_rank
result_all = open('baidu_rank_20190808.txt','w')



def requests_headers():
    head_connection = ['keep-alive']
    head_accept = ['text/html,application/xhtml+xml,*/*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5','en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = ['Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                       'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                       'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0',
                       'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11',
                       'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
                       'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30)',
                       'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; .NET CLR 1.1.4322)',
                       'Opera/9.20 (Windows NT 6.0; U; en)',
                       'Mozilla/4.0 (compatible; MSIE 5.0; Windows NT 5.1; .NET CLR 1.1.4322)',
                       'Opera/9.00 (Windows NT 5.1; U; en)',
                       'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 8.50',
                       'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 8.0',
                       'Mozilla/4.0 (compatible; MSIE 6.0; MSIE 5.5; Windows NT 5.1) Opera 7.02 [en]',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.5) Gecko/20060127 Netscape/8.1']

#    #choose random header above 
#            'Connection':head_connection[random.randrange(0,len(head_connection))],
#        'Accept':head_accept[0],
#        'Accept-Language':head_accept_language[random.randrange(0,len(head_accept_language))],
#        'User-Agent':head_user_agent[random.randrange(0,len(head_user_agent))],
    header = {
        'Connection':'keep-alive',
        'Accept':random.SystemRandom().choice(head_accept),
        'Accept-Language':random.SystemRandom().choice(head_accept_language),
        'User-Agent':random.SystemRandom().choice(head_user_agent),
        'cookie':'isworkdesign=1; Hm_lvt_a620c01aafc084582f0ec24d96b73ad8=1560493309; isphonetan=1; c_mark=7f992cc2017e6881f1acb6858a4bb635; loginkid=3; auth_token=z8jJNLuNyaZOcnvjBoi7kMl7dXNRrHKjGJF1BGwKFpLvsGrMk4FYS1p6EK0n2QyiQHOLl3FLyui87AgfxQ70iA; jumpurl=%2Fmuban%2Fqwpopnkv.html; Hm_lpvt_a620c01aafc084582f0ec24d96b73ad8=1560822422'
    }
#    print('headers.py connection Success!')
    return header

def update_data(id,rank,url):
    db = pymysql.connect(
        host = "192.168.1.91",
        port = 3306,
        user = "runping",
        passwd = "keyword",
        db = "ishare_keyword",
        charset = "utf8"
    )
    cursor = db.cursor()
    sql_up = "UPDATE `baidu_rank_20190808` SET `%s` = '%s',`%s` = '%s' WHERE `id` = '%s'"%(colum_rank,rank,colum_url,url,id)
    try:
        cursor.execute(sql_up)
        db.commit()
        print ('%s Update Success'%id)
        result_all.write('%s\t%s\t%s\n'%(id,rank,url))
    except:
        db.rollback()
        print ('%s Update Error'%id)
    db.close()
    return


def extract_url(raw):
    patten  = r'href = "(.*?)"'
    url = re.findall(re.compile(patten,re.S), raw)
    return url

def extract_id(raw):
    patten  = r'id="(.*?)" srcid='
    url = re.findall(re.compile(patten,re.S), raw)
    return url
    
def get_real_url(url):
    req = requests.get(url, headers=requests_headers(), timeout=5)
    req.encoding = 'utf-8'
    ids = r'<input type="hidden" id="checkip_fid" value="(.*?)"/>'
    out = re.findall(re.compile(ids,re.S), req.text)
    return out[0]


def work_baidu(id,word,target_fileid):
#    for i_ in range(3):
    try:
        id,rank,target_fileid = baidu_rank(id,word,target_fileid)
        print (id,rank,target_fileid)
        if rank == '' or len(rank) == 0:
            print (id,'Get None Rank')
        else:
            update_data(id,rank,target_fileid)
            time.sleep(0.5)
    except:
        print (id,'Get Rank Error')
def baidu_rank(id,word,target_fileid,retry=3):
    url=(r"https://www.baidu.com/s?wd={0}&rn=50".format(word) )
    rank = ''
    try:
        r = requests.get(url, headers=requests_headers(), timeout=5)
        r.encoding = 'utf-8'
        url_get = r'<div class="result c-container(.*?)&nbsp;-&nbsp'
        download_url = re.findall(re.compile(url_get,re.S), r.text)
        time.sleep(0.3)
        if '爱问共享资料' not in download_url:
            if retry != 0:
                time.sleep(0.2)
                return baidu_rank(id,word,target_fileid,retry-1)

        for x in download_url:
            if '爱问共享资料' in x:
                url1 = extract_url(x)
                id1 = extract_id(x)
                aaa = get_real_url(url1[0])
                time.sleep(1)
                if aaa == target_fileid:
                    rank = id1[0] 
    #                    print(id1,get_real_url(url1[0]))
                    print(id + '\t' + word + '\t' + rank + '\t' + target_fileid)
                    return id,rank,target_fileid
                    break
        else:       
            rank = '50+'
            url = ''
            results = id + '\t' + word + '\t' + '50+' + '\t' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n'
            print (results)
            return id,rank,target_fileid
            
    except:
        if retry == 0:
            return id,rank,target_fileid


     
if __name__ == '__main__' :
    
    db = pymysql.connect(
        host = "192.168.1.91",
        port = 3306,
        user = "runping",
        passwd = "keyword",
        db = "ishare_keyword",
        charset = "utf8"
    )
    cursor = db.cursor()
    sql_co = "ALTER TABLE `baidu_rank_20190808` ADD `%s` varchar(255), ADD `%s` varchar(255) DEFAULT NULL"%(colum_rank,colum_url)
    sql_fech = "SELECT id,word,target_fileid FROM baidu_rank_20190808 WHERE ﻿type = 'baidu' AND (`%s` IS NULL OR `%s` = '50+')"%(colum_rank,colum_rank)  
    try:
        cursor.execute(sql_co)
        #colum_rank = '0115'
        #colum_url = 'url0115'
    except:
        print ('Add Colum Error')
#    p = multiprocessing.Process(target=work_baidu)
    for i_ in range(6):
        pool = Pool(5)
        cursor.execute(sql_fech)
        Datas = cursor.fetchall()
        id = []
        word = []
        target_fileid = []
        for data in Datas:
            id.append(str(data[0]))
            word.append(str(data[1]))
            target_fileid.append(str(data[2]))
        task = [*zip(id,word,target_fileid)]
    #    except:
    #        print ('Fetchall Error')
        with Pool(10) as pool:
            pool.starmap(work_baidu,iterable=task) 
        pool.close()
        pool.join()
        
    db.close()
