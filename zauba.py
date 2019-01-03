import urllib3, lxml , csv
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from multiprocessing import Pool
from bs4 import BeautifulSoup
from functools import partial

#------Section:1 - Scraping------
#This section takes of care of all the Scraping bussiness of the crawler.
def extractUrl(links):
    #This function is used to get the link from the <a> tag. 
    #Returns the extracted url.
    url = links[0].get('href')
    return(url)

def directorsInformation(url, searchDepth):
    #This function is used to extract tabel data from the 'Node'.
    #Returns a list containing 'node-data' and a list containing all the 'branch - urls'.

    global visitedUrl

    #Vsiting the unvisited Url
    if url in visitedUrl:
        #Will return None for visited Url
        pass
    else:
        #Visit Url
        print("Scraping Directors Information at: ",url)

        http = urllib3.PoolManager()
        response = http.request('GET',url)

        visitedUrl.add(url) #Adding url to visited Urls.

        soup = BeautifulSoup(response.data,'lxml') #Returns lxml parsed content. 
         
        data = [] #To store the row content
        links = [] #To store all the branch Urls

        table = soup.find('table', attrs={'class':'table'}) #Looking for 'table'. Will return the first table of the page.
        #Since the page contains two tables and we only need the first one.

        rows = table.find_all('tr') #Getting all the rows in a table
        rows.pop(0) #Removing the table heading row from the rows.

        for row in rows:
            columns = row.find_all('td') #Getting all the columns in a row.
            url = extractUrl(row.find_all('a')) #Getting the branch link of a row.
            links.append(url) #Updating links[]
            
            text = [] #To store all the column text
            for column in columns:
                columnText = column.text.strip() #Extracting text from a column 
                text.append(columnText)

            text.append(url) #Adding url to column text
            text.append(searchDepth) #Adding the current searchDepth to the column text
            
            data.append(text) #Adding column to data

        return(data,links)

def getAssociatedUrls(url):
    #This function is used to visit a 'branch' and extract 'child - nodes'.
    #Returns a list containing all the child-nodes urls.
    global visitedUrl

    #Vsiting the unvisited Url
    if url in visitedUrl:
        #Will return None for visited Url
        pass
    else:
        #Visit url
        print("Scraping Companys Associated at: ",url)

        http = urllib3.PoolManager()
        response = http.request('GET',url)

        visitedUrl.add(url) #add Url to visited

        soup = BeautifulSoup(response.data,'lxml') #Returns lxml parsed content.

        links = [] #List to store all 'child-nodes'

        table = soup.find('table', attrs={'class':'table'}) #Looking for 'table'. Will return the first table of the page.
        #Since the page contains two tables and we only need the first one.

        rows = table.find_all('tr')#Getting all the rows in a table
        rows.pop(0) #Removing the table heading row from the rows.

        for row in rows:
            url = extractUrl(row.find_all('a')) #Extracting url from the row
            #The url extracted is the company's url but we need 'company-directors' url.
            url = url.replace("/company/","/company-directors/") #'Making 'company-url' from 'company' url. 
            
            links.append(url)

        return(links)

#------End of Section-1------

#------Section:2 - MultiProcessing------
#The above functions(directorsInformation, getAssociatedUrls) take one Url at a time and process it.
#Runing them one after another for any depth>1 was very time consuming.
#Hence to Scrape more than one Url at a time and speed-up the process Multi-Processing is implemented below.
def runQueue(queue,searchDepth):
    #This function will run the mulitple instances of directorsInformation at the sametime.
    #Will return lists of data and links from each process.

    NO_OF_URLS_AT_A_TIME = 30 # Depending on your machine decide how many process you want to run at at time.
    pool = Pool(NO_OF_URLS_AT_A_TIME)

    directorsInformationWithDepth = partial(directorsInformation, searchDepth=searchDepth) #Passing current searchDepth.

    #Mapping data from Multiple Processes
    records = pool.map(directorsInformationWithDepth, queue)
    
    #Terminate pool.
    pool.terminate()
    pool.join()

    data = [] #To store all the data from each Process.
    links = [] #To Store all the links from each Process.

    for item in records:
        if item!=None: #item can be None for already visited Urls.
            data = data + item[0]
            links = links + item[1]

    return(data, links)

def runBranchQueue(branchQueue):
    #This function will run the mulitple instances of branchQueue at the sametime.
    #Will return lists links from each process.

    NO_OF_URLS_AT_A_TIME = 30 # Depending on your machine decide how many process you want to run at at time.
    pool = Pool(NO_OF_URLS_AT_A_TIME)

    #Mapping data from Multiple Processes
    links = pool.map(getAssociatedUrls, branchQueue)
    
    #Terminate pool.
    pool.terminate()
    pool.join()
    
    return(links)

#------End of Section-2------

#------Section:3 - Crawler------
#This section is the brain of the Crawler. Algorithm is implemented to crawl data in a desired fashion.
def crawler(totalDepth,startUrl):
    #This function is used to implement the crawling algorithm.

    dataFrame = [] #To store complete dataFrame of the crawler
    queue = [] #To store nodes in the queue. Nodes will be visited in BFS like pattern.
    branchQueue = [] #To store branches in the queue. Branches will be visited in BFS like pattern.
    
    depth = totalDepth
    queue.append(startUrl) #Add the startURL to queue for visiting.

    while(True):
        print("Current Depth:", totalDepth-depth+1)
        
        if queue!=[]:
            #Visit all nodes in the queue
            data,branches = runQueue(queue, totalDepth-depth+1)
            dataFrame = dataFrame + data
            branchQueue = branchQueue + branches
            queue = [] #Empty queue as all the nodes are visited

        else:
            depth = depth-1 #Decrease depth.
            if depth == 0:
                return dataFrame
            else:
                #Visit all branches in the branchQueue
                nodeLinks = runBranchQueue(branchQueue)

                for link in nodeLinks:
                    if link!=None:
                        queue = queue + link
                branchQueue = [] #Empty branchQueue as all the nodes are visited

#------End of Section-3------

#------Section:4 - Storing------
#This sections contains a function to store dataFrame in the csv file.
def writeToCsv(dataFrame):
    #Function to open a 'csv' file and add data to it from dataFrame.
    print("Done Scraping. Creating csv.")
    with open('output.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['DIN', 'Director Name', 'Designation', 'Appointment Date', 'URL','Search Depth'])
        for data in dataFrame:
            writer.writerow(data)
    csv_file.close()

#------End of Section-3------

#------Main------
visitedUrl = set()
if __name__ == "__main__":
    #depth = 3
    #tartUrl = 'https://www.zaubacorp.com/company-directors/DR-REDDY-S-LABORATORIES-LTD/L85195TG1984PLC004507'
    
    depth = int(input("Enter depth: "))
    startUrl = input("Enter statUrl: ")

    print("Crawling Started!!!")
    print("Status:")

    #dataFrame = crawler(depth, startUrl)
    #writeToCsv(dataFrame)

    print("Crawling Completed!!!")