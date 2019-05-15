import requests,time,itchat,json
from config import STARTHEADER
import pymysql, traceback
import datetime
from sqlalchemy import create_engine
def start(starttime,endtime,engine,tick):
    #print(starttime)
    res=None
    header=STARTHEADER
    while True:
        try:
           #print(333)
           res = requests.get('https://ad.toutiao.com/marco/account/get_majordomo_binded_account_stat_info/?page=1&start_time='+starttime+'&end_time='+endtime+'&q=',headers=header).text
           print(res)
        except  Exception as e:
            traceback.print_exc()
            pass
        if '"status": "fail"' in res:
            #给微信发送消息通知维护人员cookie异常
            #itchat.send('更换cookie','filehelper');
            header={
                         'Cookie':input("请输入cookie:"),
                         'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
                    }
        else:
            break;

    total_page=int(json.loads(res)['data']['page_info']['total_page']);
    print("共"+str(total_page)+"页代理商页面")
    lmsg=json.loads(res)['data']['list']
    for data in lmsg:
        id1=str(data['id'])
        nam=str(data['name'])
        print(id1)
        name1=data['name']
        coo='part=stable; tt_webid=6691104647199573507; csrftoken=t9mwb5bQjkcCat1eIXTtPel0lcSbTxlC; gr_user_id=a50f7614-8252-46d1-b443-acb2a4b6039b; ccid=2c67821c0d5f87bc31e03644031bee20; grwng_uid=5265cb1d-d601-4a41-976e-960da1091be7; sso_uid_tt=df64cf50120f3cfbf27e14b4ab8eaf34; toutiao_sso_user=715f6d9faa421bf6152cea69cd70a849; login_flag=19cd370fcbdc4d6e3af4e9ef141f4823; sid_tt=c6c4345d10f9b4088e004763c46f8920; uid_tt=292b039cd463cf835f4f778962bb61a284ad9bcf6ad9582008d3721d6a7f551d; sessionid=c6c4345d10f9b4088e004763c46f8920; gr_session_id_9194a4ba15b63f36=3b7246d5-3de1-416e-a767-9314ba5ba520; gr_cs1_3b7246d5-3de1-416e-a767-9314ba5ba520=advertiser_id%3A109101412129; gr_session_id_9194a4ba15b63f36_3b7246d5-3de1-416e-a767-9314ba5ba520=true; acsessionid=b371cad0f0cb4f68aca637580a2ebb29; sid_guard="c6c4345d10f9b4088e004763c46f8920|1557894665|15552000|Mon\054 11-Nov-2019 04:31:05 GMT"; aefa4e5d2593305f_gr_last_sent_sid_with_cs1=060e6baa-5e6c-442f-b311-4a4f89702778; aefa4e5d2593305f_gr_last_sent_cs1='+str(id1)+'; aefa4e5d2593305f_gr_cs1='+str(id1)+'; aefa4e5d2593305f_gr_session_id=9ff6757b-d42b-4043-b318-3a76a6db8d86; aefa4e5d2593305f_gr_session_id_060e6baa-5e6c-442f-b311-4a4f89702778=true; __tea_sdk__user_unique_id='+str(id1)+'; __tea_sdk__ssid=87ecd276-cb81-4598-98f5-54642c9332e7'
        header1={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
            'Cookie':coo
        }
        contents=None
        datalist=None
        while True:
            try:
                contents=requests.get('https://ad.toutiao.com/statistics/data_v2/ad_stat/?page=1&limit=20&st='+starttime+'&et='+endtime+'&landing_type=0&status=no_delete&pricing=0&search_type=2&keyword=&sort_stat=&sort_order=1',headers=header1).text
                datalist=json.loads(contents)['data']['table']['ad_data']
                break
            except Exception as e:
                traceback.print_exc()
                print(contents)
                pass
        #print(contents)
        pagecount = json.loads(contents)['data']['table']['pagination']['page_count']
        #print(nam+"共"+str(pagecount)+"页推广账户列表"+contents)
        print(nam + "共" + str(pagecount) + "页推广账户列表" + str(contents))
        time.sleep(5)
        #exit(0)
        for d in datalist:
            campaign_name=d['campaign_name']
            campaign_id=d['campaign_id']
            show=d['stat_data']['show']
            pre_interactive_cost=d['stat_data']['stat_cost']
            valid_play=d['stat_data']['valid_play']
            total_play=d['stat_data']['total_play']
            ctr=d['stat_data']['ctr']
            click=d['stat_data']['click']
            cpc=d['stat_data']['cpc']
            cpm=d['stat_data']['cpm']
            ad_id=d['ad_id']
            valid_play_rate=d['stat_data']['valid_play_rate']
            convert=d['stat_data']['convert']
            conversion_cost=d['stat_data']['conversion_cost']
            convert_rate=d['stat_data']['convert_rate']
            insertData(engine, campaign_name, campaign_id, show, pre_interactive_cost, valid_play,total_play, ctr, click, cpc, cpm, ad_id, valid_play_rate, convert,conversion_cost, convert_rate,starttime,tick=tick,nams=nam)
            print(campaign_name,campaign_id,show,pre_interactive_cost,valid_play,total_play,ctr,click,cpc,cpm,ad_id,valid_play_rate,convert,conversion_cost,convert_rate)

            #print(d)
        if pagecount > 1:
            for pg in range(2, pagecount + 1):

                while True:
                    try:
                        pcontents = requests.get('https://ad.toutiao.com/statistics/data_v2/ad_stat/?page=' + str(pg) + '&limit=20&st=' + starttime + '&et=' + endtime + '&landing_type=0&status=no_delete&pricing=0&search_type=2&keyword=&sort_stat=&sort_order=1',headers=header1).text
                        datalist1 = json.loads(pcontents)['data']['table']['ad_data']
                        pp=json.loads(pcontents)['data']['table']['pagination']['page']
                        print(nam+"信息推广第" + str(pp) + '页')
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
                    insertData(engine, campaign_name, campaign_id, show, pre_interactive_cost, valid_play,total_play, ctr, click, cpc, cpm, ad_id, valid_play_rate, convert,conversion_cost, convert_rate,starttime,tick=tick,nams=nam)
                    print(campaign_name, campaign_id, show, pre_interactive_cost, valid_play, total_play, ctr, click,cpc, cpm, ad_id, valid_play_rate, convert, conversion_cost, convert_rate)
    if total_page>1:
        for p in range(2, total_page + 1):
            #print('第'+str(p)+'页')
            res1=None
            while True:
                res1 = requests.get('https://ad.toutiao.com/marco/account/get_majordomo_binded_account_stat_info/?page='+str(p)+'&start_time='+starttime+'&end_time='+endtime+'&q=',headers=header).text
                if '"status": "fail"' in res1:
                    header = {
                        'Cookie': input("请输入cookie:"),
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
                    }
                else:
                    break;

            lmsg1 = json.loads(res)['data']['list']
            ps=json.loads(res)['data']['page_info']['page']
            print(nam+"第"+str(ps)+"页代理商页面")
            time.sleep(5)
            for data in lmsg1:
                id1 = str(data['id'])
                nam=str(data['name'])
                coo='tt_webid=6688471041628603915; gr_user_id=f2c8f801-0ccc-46e9-bd9b-24a8964974f3; grwng_uid=a0ef3cba-22e7-4e75-aa11-feca3da501a8; ccid=9675adb617ed183d125446be538fe56c; sid=1dfa9be647a778322b0cf15267c7bfb4; csrftoken=NB7gIavIMgXQQc2inpMyv7A5Nj1ymFwi; sso_uid_tt=702263f4f1b2be5af4522b0784ef67a6; toutiao_sso_user=b2eb9aa83078a07bb9d1b9ff9b157bba; login_flag=2d72a3081adcdc57aef752f1847cae32; sid_tt=8fa616812d29a7d96a92b595a0d8c511; uid_tt=0f7332bc1c215be4daf102f47b5f2427ccc1dca29bc0b84e508a1102b2003379; sessionid=8fa616812d29a7d96a92b595a0d8c511; gr_session_id_9194a4ba15b63f36=80a4b093-5b91-420f-8edc-240fb651df31; gr_cs1_80a4b093-5b91-420f-8edc-240fb651df31=advertiser_id%3A109101412129; gr_session_id_9194a4ba15b63f36_80a4b093-5b91-420f-8edc-240fb651df31=true; sid_guard="8fa616812d29a7d96a92b595a0d8c511|1557710133|15552000|Sat\054 09-Nov-2019 01:15:33 GMT"; acsessionid=4ec09c3cc8e9454bb51014cba9d3819c; part=stable; aefa4e5d2593305f_gr_last_sent_sid_with_cs1=7e9290a8-2bb3-4935-8202-623e5e2613ff; aefa4e5d2593305f_gr_last_sent_cs1='+str(id1)+'; aefa4e5d2593305f_gr_cs1='+str(id1)+'; aefa4e5d2593305f_gr_session_id=7e9290a8-2bb3-4935-8202-623e5e2613ff; aefa4e5d2593305f_gr_session_id_7e9290a8-2bb3-4935-8202-623e5e2613ff=true; __tea_sdk__user_unique_id='+str(id1)+'; __tea_sdk__ssid=a99e3192-7b7d-482c-8591-847d676b0873'
                header1 = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
                    'Cookie': coo
                }
                contents = None
                datalist = None
                while True:
                    try:
                        contents = requests.get( 'https://ad.toutiao.com/statistics/data_v2/ad_stat/?page=1&limit=20&st='+starttime+'&et='+endtime+'&landing_type=0&status=no_delete&pricing=0&search_type=2&keyword=&sort_stat=&sort_order=1',headers=header1).text
                        datalist = json.loads(contents)['data']['table']['ad_data']
                        break
                    except Exception as e:
                        pass
                pagecount=json.loads(contents)['data']['table']['pagination']['page_count']
                print(nam+"共" + str(pagecount) + "页推广账户列表"+str(contents))
                time.sleep(5)
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
                    insertData(engine, campaign_name, campaign_id, show, pre_interactive_cost, valid_play,total_play, ctr, click, cpc, cpm, ad_id, valid_play_rate, convert,conversion_cost, convert_rate,starttime,tick=tick,nams=nam)
                    print(campaign_name, campaign_id, show, pre_interactive_cost, valid_play, total_play, ctr, click,cpc, cpm, ad_id, valid_play_rate, convert, conversion_cost, convert_rate)
                if pagecount>1:
                    for pg in range(2,pagecount+1):
                        #print("信息第"+str(pg)+'页')
                        while True:
                            try:
                                pcontents = requests.get('https://ad.toutiao.com/statistics/data_v2/ad_stat/?page='+str(pg)+'&limit=20&st=' + starttime + '&et=' + endtime + '&landing_type=0&status=no_delete&pricing=0&search_type=2&keyword=&sort_stat=&sort_order=1',headers=header1).text
                                datalist1 = json.loads(pcontents)['data']['table']['ad_data']
                                pp = json.loads(pcontents)['data']['table']['pagination']['page']
                                print(nam+"信息第" + str(pp) + '页')
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
                            insertData(engine, campaign_name, campaign_id, show, pre_interactive_cost, valid_play,total_play, ctr, click, cpc, cpm, ad_id, valid_play_rate, convert,conversion_cost, convert_rate,starttime,tick=tick,nams=nam)
                            print(campaign_name, campaign_id, show, pre_interactive_cost, valid_play, total_play, ctr,click, cpc, cpm, ad_id, valid_play_rate, convert, conversion_cost, convert_rate)



def insertData(engine,campaign_name,campaign_id,show,pre_interactive_cost,valid_play,total_play,ctr,click,cpc,cpm,ad_id,valid_play_rate,convert,conversion_cost,convert_rate,data_time,tick,nams):
    try:
       engine.execute('INSERT INTO `jinritoutiao_data` (campaign_name,campaign_id,`show`,pre_interactive_cost,valid_play,total_play,ctr,click,cpc,cpm,ad_id,valid_play_rate,`convert`,conversion_cost,convert_rate,data_time,tick,main_name,`crawl_time`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,SYSDATE())',(campaign_name,str(campaign_id),str(show),str(pre_interactive_cost),str(valid_play),str(total_play),str(ctr),str(click),str(cpc),str(cpm),str(ad_id),str(valid_play_rate),str(convert),str(conversion_cost),str(convert_rate),str(data_time),tick,nams))
       print("插入成功")
    except Exception as e:
        traceback.print_exc()
        print("插入失败")
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
    while True:
         start(str('2019-05-12'), str('2019-05-12'), engine=engine, tick=tick)
         print("今天采集完成！")
         tick=tick+1
         time.sleep(86400)









