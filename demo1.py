import requests,time,itchat,json
from config import STARTHEADER
import pymysql, traceback
import datetime
from sqlalchemy import create_engine
from selenium import webdriver
from pyquery import PyQuery as pq

class TouTiao:
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.driver.get('https://ad.toutiao.com/pages/login/index.html')
        time.sleep(30)

    def start(self):
        main_contents=self.driver.page_source;
        psize=int(pq(main_contents)('.byte-pagination-total-records').text().replace('条记录','').replace('共','').strip())%20
        if psize>0:
            psize=int(pq(main_contents)('.byte-pagination-total-records').text().replace('条记录','').replace('共','').strip())//20+1
        else:
            psize=int(pq(main_contents)('.byte-pagination-total-records').text().replace('条记录','').replace('共','').strip())//20
        print(psize)

        advertisers=self.driver.find_elements_by_class_name('advertiser-name')
        currhandle=self.driver.current_window_handle;
        print(currhandle)
        for  i in advertisers:
            i.click();
            print(2)
            handles=self.driver.window_handles
            for hand in handles:
                if hand!=currhandle:
                    self.driver.switch_to.window(hand)
                    time.sleep(10)
                    break;
            contents = self.driver.page_source;
            print(self.driver.current_window_handle)
            self.driver.close()
            self.driver.switch_to.window(currhandle)

TouTiao().start()










