#!/usr/bin/env python
# -*- coding: utf-8 -*-
from Queue import Queue
import re
import urllib2
import time
from bs4 import BeautifulSoup
import socket
import sys
import math
import Distribute
import json
import chardet
import gzip
import codecs
from StringIO import StringIO
timeout = 15
socket.setdefaulttimeout(timeout)
inputq=Queue(maxsize=0)
outputq=Queue(maxsize=0)
regexp = r"ubp = ({.*?});"
regexp1=r'px:"(.*?)"'
regexp2=r'py:"(.*?)"'
headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',}

#下载网页
def downloadhtml(url, headers,RepeatCount=5):
    try:
        time.sleep(1)
        req = urllib2.Request(url=url, headers=headers)
        response = urllib2.urlopen(req)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO(response.read())
            content = gzip.GzipFile(fileobj=buf).read()
        else:
            content=response.read()
    except urllib2.URLError as e:
        content = None
        if (RepeatCount > 0):
            if hasattr(e, 'code') and 500 <= e.code < 600:    #服务器端错误
                return downloadhtml(url, headers, RepeatCount - 1)
    except socket.error:                                      #底层socket错误
        content = None
        errno, errstr = sys.exc_info()[:2]
        if errno == socket.timeout:
            print "There was a timeout"
        else:
            print "There was some other socket error"
        time.sleep(2)
        if (RepeatCount > 0):
            return downloadhtml(url, headers, RepeatCount - 1)
    except:                                                   #其它错误
        content=None
        if (RepeatCount > 0):
            return downloadhtml(url, headers, RepeatCount - 1)
    return content

#获取每个地区小区的总页面数
def get_pages(url,ReC=2):
    content=downloadhtml(url,headers)
    if content is not None:
        soup=BeautifulSoup(content)
        xiaoqu_count=soup.select(".findplotNum")[0].get_text()
        if xiaoqu_count==str(0) and ReC>0:
            return get_pages(url,ReC-1)
        print xiaoqu_count
        return int(math.ceil(int(xiaoqu_count)/20.0))
    return 0

def WriteToFile(xiaoqu_info_list):
    f1 = codecs.open(filename,"w", "utf8")
    for xiaoqu_info in xiaoqu_info_list:
        f1.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
                %(xiaoqu_info[0],xiaoqu_info[1],xiaoqu_info[2],xiaoqu_info[3],xiaoqu_info[4],
                  xiaoqu_info[5],xiaoqu_info[6],xiaoqu_info[7],xiaoqu_info[8]
                  ))
    f1.close()

#线程执行的任务
def task():
    while not inputq.empty():
        xiaoqu_info=inputq.get()
        id=xiaoqu_info[0]
        url=qianzhui_url+"/newsecond/map/newhouse/ShequMap.aspx?newcode={}".format(id)
        content=downloadhtml(url,headers)
        if content is None:
            print url
            inputq.put(xiaoqu_info)
            continue
        try:
            #content = content.decode("gbk").encode("utf8")
            latitude=re.findall(regexp2, content)[0]
            longitude=re.findall(regexp1, content)[0]
        except Exception,e:
            inputq.put(xiaoqu_info)
            print url
            continue
        xiaoqu_info.append("")
        xiaoqu_info.append("")
        xiaoqu_info.append("")
        if xiaoqu_info[2]=="":
            xiaoqu_info[2]=u"未知"
        xiaoqu_info[0],xiaoqu_info[1],xiaoqu_info[2],xiaoqu_info[3],xiaoqu_info[4],xiaoqu_info[5],xiaoqu_info[6],xiaoqu_info[7],xiaoqu_info[8]=xiaoqu_info[1],xiaoqu_info[3],xiaoqu_info[6],xiaoqu_info[7],xiaoqu_info[4],latitude,longitude,xiaoqu_info[2],xiaoqu_info[5],
        outputq.put(xiaoqu_info)






all_url_list = []
temp_info=[]
f=codecs.open("city.dat","r","utf8")
for each0 in f:
    list_temp= each0.strip().split("\t")
    qianzhui_url = list_temp[0]
    cityname=list_temp[0].split(".")[1]
    filename = "{}_xiaoqu_info.dat".format(cityname)
    province=list_temp[1]
    city=list_temp[2]
    url = qianzhui_url + "/housing/__0_0_1_0_1_0_0/"
    pg=get_pages(url)
    if pg==0:
        continue
    #print url
    if pg>100:   #继续细分为多个地区。。。。列表最多只显示100页
        content = downloadhtml(url, headers)
        soup = BeautifulSoup(content)
        list = soup.select(".con")[0]
        list1 = list.find_all("a")      #每个城市的小地区
        all_url_list = []
        temp_info=[]
        for each in list1:     #每个小地区
            url = qianzhui_url + each["href"]
            print url
            pages = get_pages(url)
            print pages
            if pages==0:
                continue
            url_list = [url[:-12] + "1_2_0_" + "{}_0_0/".format(str(i)) for i in xrange(1, pages + 1)]
            for ul in url_list:
                inputq.put(ul)
            while not inputq.empty():
                name_list = []
                xiaoqu_url_list = []
                addr_list = []
                avg_list = []
                useway_list = []
                each1 = inputq.get()
                content = downloadhtml(each1, headers)
                if content is None:
                    print each1
                    inputq.put(each1)
                    continue
                content = content.decode("gbk").encode("utf8")
                find = re.findall(regexp, content)[0]
                find = json.loads(find)
                try:
                    find = find["vwx.showhouseid"].split(',')
                except:
                    inputq.put(each1)
                    continue
                soup = BeautifulSoup(content)
                temp_xioqu_info = soup.select(".plotListwrap")
                avg_list_info = soup.select(".priceAverage")
                for k in temp_xioqu_info:
                    xiaoqu_info = k.dd.find_all("p")
                    name_list.append(xiaoqu_info[0].a.get_text().strip())
                    useway_list.append(xiaoqu_info[0].span.get_text().strip())
                    xiaoqu_url_list.append(xiaoqu_info[0].a["href"])
                    addr_list.append(xiaoqu_info[1].get_text().strip())
                for ap in avg_list_info:
                    avg_list.append(ap.get_text().strip())
                for each2, name, addr, url, avg, useway in zip(find, name_list, addr_list, xiaoqu_url_list, avg_list,useway_list):
                    if each2 not in all_url_list:
                        #print each2
                        all_url_list.append(each2)
                        temp_info.append([each2, name, addr, url, avg, useway,province,city])
        for xiaoqu_info in temp_info:
            inputq.put(xiaoqu_info)
        dis = Distribute.Distribute(8, task)
        dis.execute()
        #task()
        new_xiaoqu_info = []
        while not outputq.empty():
            new_xiaoqu_info.append(outputq.get())
        WriteToFile(new_xiaoqu_info)
    else:
        url_list = [url[:-12] + "1_2_0_" + "{}_0_0/".format(str(i)) for i in xrange(1, pg + 1)]
        print len(url_list)
        all_url_list = []
        temp_info = []
        for ul in url_list:
            inputq.put(ul)
        while not inputq.empty():
            name_list = []
            xiaoqu_url_list = []
            addr_list = []
            avg_list = []
            useway_list = []
            each1 = inputq.get()
            content = downloadhtml(each1, headers)
            if content is None:
                print each1
                inputq.put(each1)
                continue
            content = content.decode("gbk").encode("utf8")
            find = re.findall(regexp, content)[0]
            find = json.loads(find)
            try:
                find = find["vwx.showhouseid"].split(',')
            except:
                inputq.put(each1)
                continue
            soup = BeautifulSoup(content)
            temp_xioqu_info = soup.select(".plotListwrap")
            avg_list_info = soup.select(".priceAverage")
            for k in temp_xioqu_info:
                xiaoqu_info = k.dd.find_all("p")
                name_list.append(xiaoqu_info[0].a.get_text().strip())
                useway_list.append(xiaoqu_info[0].span.get_text().strip())
                xiaoqu_url_list.append(xiaoqu_info[0].a["href"])
                addr_list.append(xiaoqu_info[1].get_text().strip())
            for ap in avg_list_info:
                avg_list.append(ap.get_text().strip())
            for each2, name, addr, url, avg, useway in zip(find, name_list, addr_list, xiaoqu_url_list, avg_list,
                                                           useway_list):
                if each2 not in all_url_list:
                    #print each2
                    all_url_list.append(each2)
                    temp_info.append([each2, name, addr, url, avg, useway, province, city])
        for xiaoqu_info in temp_info:
            inputq.put(xiaoqu_info)
        dis = Distribute.Distribute(8, task)
        dis.execute()
        # task()
        new_xiaoqu_info = []
        while not outputq.empty():
            new_xiaoqu_info.append(outputq.get())
        WriteToFile(new_xiaoqu_info)
f.close()
