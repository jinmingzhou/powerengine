import requests,time,itchat,json
from config import STARTHEADER
import pymysql, traceback
import datetime
from sqlalchemy import create_engine
from selenium import webdriver
from pyquery import PyQuery as pq

class TouTiao:
    def __init__(self):
        host = 'yatsenglobal-data.mysql.polardb.rds.aliyuncs.com'
        port = '3306'
        user = 'python_spider'
        pwd = 'ubPUG2OqjRFfXtke'
        db = 'kol_spider'
        sqlurl = 'mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8'.format(user=user, host=host, pwd=pwd, port=port, db=db);
        self.engine = create_engine(sqlurl)
        self.driver = webdriver.Chrome()
        self.driver.get('https://ad.toutiao.com/pages/login/index.html')
        time.sleep(30)

    def insertData(self,campaign_name, campaign_id, show, pre_interactive_cost, valid_play, total_play, ctr, click,cpc, cpm, ad_id, valid_play_rate, convert, conversion_cost, convert_rate, data_time, tick, nams):
        try:
            self.engine.execute('INSERT INTO `jinritoutiao_data` (campaign_name,campaign_id,`show`,pre_interactive_cost,valid_play,total_play,ctr,click,cpc,cpm,ad_id,valid_play_rate,`convert`,conversion_cost,convert_rate,data_time,tick,main_name,`crawl_time`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,SYSDATE())',(campaign_name, str(campaign_id), str(show), str(pre_interactive_cost), str(valid_play), str(total_play),str(ctr), str(click), str(cpc), str(cpm), str(ad_id), str(valid_play_rate), str(convert),str(conversion_cost), str(convert_rate), str(data_time), tick, nams))
            print("插入成功")
        except Exception as e:
            traceback.print_exc()
            print("插入失败")

    def start(self,starttime='2019-05-12',endtime='2019-05-12',ticks=None):
        main_contents=self.driver.page_source;
        psize=int(pq(main_contents)('.byte-pagination-total-records').text().replace('条记录','').replace('共','').strip())%20
        if psize>0:
            psize=int(pq(main_contents)('.byte-pagination-total-records').text().replace('条记录','').replace('共','').strip())//20+1
        else:
            psize=int(pq(main_contents)('.byte-pagination-total-records').text().replace('条记录','').replace('共','').strip())//20
        print(psize)
        for page in range(1,psize+1):
            if page>1:
                time.sleep(2)
                next=len(self.driver.find_element_by_class_name('byte-pagination').find_elements_by_css_selector("li"))
                nex=self.driver.find_element_by_class_name('byte-pagination').find_elements_by_css_selector("li")[next-1]
                nex.click()
                pass
            #print("1312321"+self.driver.page_source)
            advertisers = self.driver.find_elements_by_class_name('advertiser-name')
            currhandle = self.driver.current_window_handle;
            #print(currhandle)
            time.sleep(3)
            while True:
                try:
                    for i in advertisers:
                        main_name = i.text;
                        i.click();
                        handles = self.driver.window_handles
                        for hand in handles:
                            if hand != currhandle:
                                self.driver.switch_to.window(hand)
                                time.sleep(5)
                                break;
                        contents = None
                        datalist = None
                        while True:
                            try:
                                self.driver.get('https://ad.toutiao.com/statistics/data_v2/ad_stat/?page=1&limit=20&st=' + starttime + '&et=' + endtime + '&landing_type=0&status=no_delete&pricing=0&search_type=2&keyword=&sort_stat=&sort_order=1')
                                contents = pq(self.driver.page_source)('body').text()
                                datalist = json.loads(contents)['data']['table']['ad_data']
                                break
                            except Exception as e:
                                traceback.print_exc()
                                print(contents)
                                pass

                        pagecount = json.loads(contents)['data']['table']['pagination']['page_count']
                        # print(nam+"共"+str(pagecount)+"页推广账户列表"+contents)
                        print(main_name + "共" + str(pagecount) + "页推广账户列表" + str(contents))
                        time.sleep(5)
                        # exit(0)
                        for d in datalist:
                            campaign_name = d['campaign_name']
                            campaign_id = d['campaign_id']
                            show = d['stat_data']['show']
                            pre_interactive_cost = d['stat_data']['stat_cost']
                            valid_play = d['stat_data']['valid_play']
                            total_play = d['stat_data']['total_play']
                            ctr = d['stat_data']['ctr']
                            click = d['stat_data']['click']
                            cpc = d['stat_data']['cpc']
                            cpm = d['stat_data']['cpm']
                            ad_id = d['ad_id']
                            valid_play_rate = d['stat_data']['valid_play_rate']
                            convert = d['stat_data']['convert']
                            conversion_cost = d['stat_data']['conversion_cost']
                            convert_rate = d['stat_data']['convert_rate']
                            self.insertData(campaign_name, campaign_id, show, pre_interactive_cost, valid_play, total_play,ctr, click, cpc, cpm, ad_id, valid_play_rate, convert, conversion_cost, convert_rate,starttime, ticks,main_name)
                            print(campaign_name, campaign_id, show, pre_interactive_cost, valid_play, total_play, ctr,
                                  click, cpc, cpm, ad_id, valid_play_rate, convert, conversion_cost, convert_rate)
                        if pagecount > 1:
                            for pg in range(2, pagecount + 1):

                                while True:
                                    try:
                                        self.driver.get(
                                            'https://ad.toutiao.com/statistics/data_v2/ad_stat/?page=' + str(
                                                pg) + '&limit=20&st=' + starttime + '&et=' + endtime + '&landing_type=0&status=no_delete&pricing=0&search_type=2&keyword=&sort_stat=&sort_order=1')
                                        pcontents = pq(self.driver.page_source)('body').text()
                                        datalist1 = json.loads(pcontents)['data']['table']['ad_data']
                                        pp = json.loads(pcontents)['data']['table']['pagination']['page']
                                        print(main_name + "信息推广第" + str(pp) + '页')
                                        time.sleep(5)
                                        break
                                    except Exception as e:
                                        pass
                                for d in datalist1:
                                    campaign_name = d['campaign_name']
                                    campaign_id = d['campaign_id']
                                    show = d['stat_data']['show']
                                    pre_interactive_cost = d['stat_data']['stat_cost']
                                    valid_play = d['stat_data']['valid_play']
                                    total_play = d['stat_data']['total_play']
                                    ctr = d['stat_data']['ctr']
                                    click = d['stat_data']['click']
                                    cpc = d['stat_data']['cpc']
                                    cpm = d['stat_data']['cpm']
                                    ad_id = d['ad_id']
                                    valid_play_rate = d['stat_data']['valid_play_rate']
                                    convert = d['stat_data']['convert']
                                    conversion_cost = d['stat_data']['conversion_cost']
                                    convert_rate = d['stat_data']['convert_rate']
                                    self.insertData(campaign_name, campaign_id, show, pre_interactive_cost, valid_play,total_play, ctr, click, cpc, cpm, ad_id, valid_play_rate, convert,conversion_cost, convert_rate, starttime,ticks, main_name)
                                    print(campaign_name, campaign_id, show, pre_interactive_cost, valid_play,
                                          total_play,
                                          ctr, click, cpc, cpm, ad_id, valid_play_rate, convert, conversion_cost,
                                          convert_rate)
                        self.driver.close()
                        self.driver.switch_to.window(currhandle)
                    break
                except Exception as e:
                    advertisers = self.driver.find_elements_by_class_name('advertiser-name')




def getYesterday():
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1)
    yesterday=today-oneday
    return yesterday

if __name__=='__main__':
    host = 'yatsenglobal-data.mysql.polardb.rds.aliyuncs.com'
    port = '3306'
    user = 'python_spider'
    pwd = 'ubPUG2OqjRFfXtke'
    db = 'kol_spider'
    sqlurl = 'mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset=utf8'.format(user=user, host=host, pwd=pwd,port=port, db=db);
    engine = create_engine(sqlurl)
    tick=1
st=TouTiao()
while True:
    st.start(ticks=tick)
    tick=tick+1;
    time.sleep(86400)












