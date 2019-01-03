# Zauba Scrapper
My first web scrapping project using Beautiful Soup4, lxml and urllib3.

**Objective:** To crawl www.zaubacorp.com to understand Director relationships.

**Background:** Zaubacorp is a website that neatly categorizes publicly available information with the registrar of Indian companies.

**Inputs:**
1. Starting URL - A starting URL which is the zaubacorp page for a company
2. Depth - A number between 1 and 5

**Output:**
Csv file containing URL, DIN, Director Name, Designation, Appointment Date, Search Depth.

## File desciption:
1. zauba.py : Python script to implement the web scrapper.
2. output.csv : Output for the [URL](https://www.zaubacorp.com/company/DR-REDDY-S-LABORATORIES-LTD/L85195TG1984PLC004507) and DEPTH = 3.
3. requirements.txt : Requirements for the above script to work. Install in virtual-environment and run the script from there.
4. firstDraft.py : This was my initial approach to the problem (without multi-processing). It's not documented but you will get an idea.

## Final Note:
I have tried my best to document the code wherever possible so that you don't have a hard time figuring out what I wanted to do.
