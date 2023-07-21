import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from common import last_bd
from utils import timer

COLS_MORNINGSTAR=["ETF","P/E1","P/B","P/S","P/CF","DY","EG","HG","SG","CFG","BG","Composite","Last_updated","UpdateMode","URL","Name"]
COLS_RATIOS=["EY","B/P","S/P","DY","Compo_Zscore"]
SECTORS =["Basic Materials","Consumer Cyclical","Financial Services", "Real Estate","Communication Services",
              "Energy","Industrials","Technology", "Consumer Defensive", "Healthcare", "Utilities"  ]


class SeleniumError(Exception):
    pass


benchmark='ACWI'
    
ratios=[]
row_dict={x:"" for x in COLS_MORNINGSTAR}
errs=[]




def sub_getETF_Selenium(driver,ETF_name,exchange='arcx'):
    if exchange[:5]=='funds':
        url=f"https://www.morningstar.com/{exchange}/{ETF_name}/portfolio"
    else:  
        foundURL=False
        EXCHANGE_LIST=set(['arcx','xnys','xnas','bats']+[exchange])
        for exc in EXCHANGE_LIST:
            url=f"https://www.morningstar.com/etfs/{exc}/{ETF_name}/portfolio"
            r = requests.get(url)
            if r.status_code==200:
                foundURL=True
                break
        if not foundURL:
            raise SeleniumError('Correct Url could not be found for: '+ETF_name)
    print(f'[+]Scrapping for {url=}')
    driver.get(url)
    wait = WebDriverWait(driver, 15)
    element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'sal-sector-exposure__sector-table')))
    element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'sal-measures__value-table')))
    
    html_source = driver.page_source
    source_data = html_source.encode('utf-8')
    soup = BeautifulSoup(source_data, "lxml")
    
    compteur = 0
    row_dict["ETF"]=ETF_name
    row_dict["Last_updated"]=last_bd
    res=soup.find(class_="mdc-security-header__name")
    # res=res.find('span')
    nom=res.text
    tmp=nom.splitlines()
    tmp=[el for el in tmp if len(el)>=3]
    row_dict["Name"]=tmp[0].strip()
    """ find and grab financial ratios """
    res=soup.find(class_='sal-measures__value-table')
    res=res.find("tbody")    
    for row in res.find_all("tr"):
        sratio=row.find_all("td")[1].text
        if sratio!="":
            try:
                ratio=float(sratio) 
            except:
                """exception for P/CF not as important as others, replace by cat """
                if row.find_all("td")[0].text.strip()=="Price/Cash Flow":
                    try:
                        ratio=float(row.find_all("td")[2].text)
                    except:
                        ratio=float('nan')
                else: 
                    ratio=float('nan')
            row_dict[COLS_MORNINGSTAR[compteur+1]]=ratio
            #print "%s \t % 6.2f" % (cols[compteur+1],ratio)           
            compteur+=1
    #tbs=soup.findAll("tr", {"class": "ng-scope"})
    """ find and grab sector composition """
    res=soup.find(class_='sal-sector-exposure__sector-table')
    res=res.find("tbody")   
    for row in res.find_all("tr"):
        cells=row.find_all("td")
        lbl=cells[0].text.strip()
        if lbl in SECTORS:
            try:
                weight=float(cells[1].text)
            except:
                weight=float('nan')
            row_dict[lbl]=weight
            compteur+=1
    row_dict["Last_updated"]=last_bd
    row_dict["UpdateMode"]='Selenium'
    row_dict["URL"]=url
    res=row_dict   
    return res


def start_driver():
    try:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--log-level=3")
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        driver.quit()       
        raise Exception("Could not create the driver") from e
         

@timer
def selenium_scrap():
    # using info from https://help.pythonanywhere.com/pages/selenium
    res=None
    try:
        driver = start_driver()
        res = sub_getETF_Selenium(driver,'QQQ', exchange='xnas')
        print(res) 
    finally:
        driver.quit()
    return res


if __name__ == "__main__":
    res = selenium_scrap()
    print('Scrapped ok')