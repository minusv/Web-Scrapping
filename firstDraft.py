import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import lxml
from bs4 import BeautifulSoup

import csv
                    
def extractUrl(links):
    url = links[0].get('href')
    return(url)

def directorsInformation(soup, searchDepth):
    data = []
    links = []

    table = soup.find('table', attrs={'class':'table'})
    rows = table.find_all('tr')
    rows.pop(0)

    for row in rows:
        columns = row.find_all('td')
        url = extractUrl(row.find_all('a'))
        links.append(url)
        
        text = []
        for column in columns:
            columnText = column.text.strip()
            text.append(columnText)

        text.append(url)
        text.append(searchDepth)
        
        data.append(text)

    return(data,links)

def associatedUrls(soup):
    links = []
    table = soup.find('table', attrs={'class':'table'})
    rows = table.find_all('tr')
    rows.pop(0)

    for row in rows:
        url = extractUrl(row.find_all('a'))
        url = url.replace("/company/","/company-directors/")
        links.append(url)

    return(links)

def visitPage(url):
    http = urllib3.PoolManager()
    response = http.request('GET',url)
    soup = BeautifulSoup(response.data,'lxml')
    return(soup)

def traverse(totalDepth,startUrl):
    finalData = []
    queue = []
    branchQueue = []
    visitedUrl = {}
    depth = totalDepth
    queue.append(startUrl)

    while(True):
        print("currentDepth:",totalDepth-depth+1)
        if queue!=[]:
            url = queue.pop(0)
            if url not in visitedUrl:
                print("NODE: ", url)
                visitedUrl[url] = True
                soup = visitPage(url)
                data,branches = directorsInformation(soup, totalDepth-depth+1)
                finalData = finalData + data
                branchQueue = branchQueue + branches
        else:
            depth = depth-1
            if depth == 0:
                return finalData
            else:
                while(branchQueue!=[]):
                    branchUrl = branchQueue.pop(0)
                    if branchUrl not in visitedUrl:
                        print("BRANCH: ", branchUrl)
                        visitedUrl[branchUrl] = True
                        soup = visitPage(branchUrl)
                        nodeLinks = associatedUrls(soup)
                        queue = queue + nodeLinks

def writeToCsv(finalData):
    with open('output1.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['DIN', 'Director Name', 'Designation', 'Appointment Date', 'URL','Search Depth'])
        for data in finalData:
            writer.writerow(data)
    csv_file.close()


if __name__ == "__main__":
    depth = 5
    startUrl = 'https://www.zaubacorp.com/company-directors/DR-REDDY-S-LABORATORIES-LTD/L85195TG1984PLC004507'
    #depth = int(input())
    #startUrl = input()

    finalData = traverse(depth, startUrl)
    writeToCsv(finalData)
    print("Task Completed!!!")