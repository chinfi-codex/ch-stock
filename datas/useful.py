# -*- coding: utf-8 -*-
import json
import time
import random
import requests
from fake_useragent import UserAgent
from PIL import Image
import io


def init_reviewer(images):
    @st.cache_data(show_spinner="解读截图ing...")
    def ocr_parse(img,extention):
        buffered = io.BytesIO()
        img.save(buffered, format=extention)
        img_str = base64.b64encode(buffered.getvalue()).decode()
        base64_str = f'data:image/{extention};base64,'+ img_str

        url ="https://api.ocr.space/parse/image"
        payload = {
            "apikey": "K81957358688957",
            "base64Image": base64_str,
            "language": "chs",
            "isTable": "true"
        }
        r = requests.post(url, data=payload)
        return r.content.decode()

    parse_texts = ''
    for image in images:
        extention = image.name.split('.')[1].lower()
        img = Image.open(image)
        width, height = img.size
        new_height = int(height * 0.8)
        cropped_image = img.crop((0, height - new_height, width, height))
        r = ocr_parse(cropped_image,extention)
        result = json.loads(r).get('ParsedResults')[0].get('ParsedText')
        parse_texts += result


def jd_product_comments(pid):
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "cookie": "__jdu=1507876332; shshshfpa=2ea021ee-52dd-c54e-1be1-f5aa9e333af2-1640075639; areaId=5; PCSYCityID=CN_0_0_0; ipLoc-djd=5-142-42547-54561; pinId=S4TjgVP4kjjnul02leqp07V9-x-f3wj7; pin=jd_60a1ab2940be3; unick=jd_60a1ab2940be3; _tp=672TNfWmOtaDFuqCPQqYycXMpi6F%2BRiwrhIuumNmoJ4%3D; _pst=jd_60a1ab2940be3; user-key=a2aaf011-2c1e-4dea-bf76-3392d16b1fb1; __jdc=122270672; wlfstk_smdl=jlwwba2gmccq62touff9evvbp3fk8gbr; ceshi3.com=000; shshshfp=4e8d45f57897e469586da47a0016f20e; ip_cityCode=142; shshshfpb=n7UymiTWOsGPvQfCup%2B3J1g%3D%3D; joyya=1647305570.1647305909.27.0kop377; __jda=122270672.1507876332.1640075637.1647314039.1647318046.22; token=d5899471c4530886f6a9658cbea3ca94,3,915176; __tk=1570759a7dd1a720b0db2dec5df8d044,3,915176; CCC_SE=ADC_Wj0UWzuXioxsiUvbIxw9PbW9q011vNMASHkfjXFO%2fZlkeGDtZUHe5qgaEpWv8RDEkCruGSGmCItsvHjIZ3aHbh9heUjNIZh6WZl9ZDfDokk66kRX6I%2by%2bDsdf4JtPOQUuULSsWOA%2fcDyP7Bb91YuHOwNnciLtS97UIKO7XA5sAd34Rf4XDKijy6Fw1DFTx%2b7izzme6YALuLp9Y%2bByC6aUTDzU9te7g1BZXPXtfGGwqu52ZVkdVId2jpxPnhX24fFD9WI9aX1qgswZ1PPZSGYKswUkqXhIf2S9aLFkjXW2n61LVzw2ZeqJRQI8QIcmi%2fF7WHOHLbWScnKwG594WIk0SRiCa0n2aEJAhVlXmzEE%2f5%2f%2bXWsKhlneTLduVs52ST5m96zdx%2bLnNGgDERqznFNu3AT5zvLcN0PyVq08n4keSv2ngLLTZK4QQJslS4he9MT3XJoEUfe9L8beZNh1239eLHYF6w4KWMCWWTfwxdCUOY%3d; unpl=JF8EAJZnNSttDEhSAkwDE0dEGAoEWw8LSh9TbjRVXV5QHFIDGwMfGhd7XlVdXhRKFR9vYxRUXlNIUw4ZBysSEXteVV1YCE0TAGlnNWRtW0tkBCsCHxMWQltTXF8LeycDZ2M1VFxZSlYGGwcTEhhObWRbXQlKFQBpYQVQbVl7VTVNbBsTEUpcVVteDENaA2tmA11bX0lWBisDKxE; __jdv=122270672|jd.idey.cn|t_2024175271_|tuiguang|e276f09debfa4c209a0ba829f7710596|1647318395561; thor=8D225D1673AA75681B9D3811417B0D325568BB2DD7F2729798D3AECF0428F59F4C39726C44E930AA2DD868FC4BCA33EA0D52228F39A68FC9F5C1157433CAACF1110B20B6975502864453B70E6B21C0ED165B733359002643CD05BDBA37E4A673AF38CC827B6013BCB5961ADA022E57DB6811E99E10E9C4E6410D844CD129071F7646EC7CE120A0B3D2F768020B044A010452D9F8ABD67A59D41880DD1991935C; 3AB9D23F7A4B3C9B=24HI5ARAA3SK7RJERTWUDZKA2NYJIXX3ING24VG466VC3ANKUALJLLD7VBYLQ3QPRYUSO3R6QBJYLVTVXBDIGJLGBA; __jdb=122270672.5.1507876332|22.1647318046; shshshsID=d7a96097b296c895558adfd840546a72_5_1647318650562",
        "referer": "https://search.jd.com/"
    }

    @st.cache_data(ttl="1day")
    def crawlProductComment(url):
        reqs = requests.get(url=url, headers=headers).text
        jsondata = json.loads(reqs)
        # 遍历商品评论列表
        comments = jsondata['comments']
        return comments

    data = []
    for i in range(0, 100):
        url = f"https://api.m.jd.com/?appid=item-v3&functionId=pc_club_productPageComments&client=pc&clientVersion=1.0.0&t=1691998913747&loginType=3&uuid=122270672.1691994024419648346878.1691994024.1691994081.1691998361.3&productId={str(pid)}&score=0&sortType=5&page={str(i)}&pageSize=10&isShadowSku=0&fold=1&bbtf=&shield="
        comments = crawlProductComment(url)
        if len(comments) <= 0:
            break
        for c in comments:
            data.append(dict(time=c.get('creationTime'),content=c.get('content')))
        time.sleep(random.randint(1, 3))
        print('-------', i)
    df = pd.DataFrame(data)
    return df


def weibo_comments(wid):
    url = f'https://weibo.com/ajax/statuses/show?id={wid}'
    header = {
        'user-agent':UserAgent().random
    }
    res = requests.get(url=url,headers=header)
    json_data = res.json()
    id = json_data['id']
    user_id = json_data['user']['idstr']

    # 获取评论
    comments = []
    max_id = ''
    while max_id != 0:
        pl_url = f'https://weibo.com/ajax/statuses/buildComments?is_reload=1&id={id}&is_show_bulletin=2&is_mix=0&max_id={max_id}&count=10&uid={user_id}'
        resp = requests.get(url=pl_url,headers=header)
        json_data = resp.json()
        max_id = json_data['max_id']
        lis = json_data['data']
        for li in lis:
            text_raw = li['text_raw']
            comments.append(text_raw)
    return comments


# if st.button("中报筛选"):
#     h1_report_df = ak.stock_yjbb_em(date="20230630")
#     #h1_report_df = h1_report_df[['股票代码','股票简称', '营业收入-同比增长', '营业收入-季度环比增长', '净利润-同比增长', '净利润-季度环比增长','销售毛利率', '所处行业', '最新公告日期']]
#     df = h1_report_df.copy()
#     filtered_df = df[
#         #(df['营业收入-同比增长'] > 10) &
#         (df['营业收入-季度环比增长'] > 0) &
#         (df['净利润-同比增长'] > 0) &
#         (df['净利润-季度环比增长'] > 0)
#     ]
#     filtered_df = filtered_df[~filtered_df['股票代码'].str.startswith('8')]
#     filtered_df = filtered_df[~filtered_df['股票代码'].str.startswith('4')]

#     increase_pct = []
#     for i,row in filtered_df.iterrows():
#         code = row.get('股票代码')

#         start_date = row.get('最新公告日期')
#         start_date = start_date - datetime.timedelta(days=2)
#         start_date = str(start_date).replace('-','')
#         try:
#             prices_df = get_K_df(code,start_date=start_date)
#             first_price = prices_df['close'].iloc[-1]  # 第一行的 'close' 值
#             last_price = prices_df['close'].iloc[0]  # 最后一行的 'close' 值
#             increase = ((first_price /last_price) -1) * 100
#         except Exception as e:
#             increase = None
#         increase_pct.append(increase)
#     filtered_df['after_increase'] = increase_pct

#     def total_value(symbol):
#         try:
#             df = ak.stock_individual_info_em(symbol=symbol)
#             total_value = df.loc[df['item'] == '总市值', 'value'].iloc[0]
#             return total_value
#         except Exception as e:
#             print (e)
#             return '-'
#     filtered_df['total_value'] = filtered_df['股票代码'].apply(total_value)
#     filtered_df.to_csv('fi.csv')
#     st.write("finish")