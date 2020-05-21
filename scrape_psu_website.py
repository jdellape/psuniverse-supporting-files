#Author: John Dellape
#Date: 5/21/2020

'''
Description: In this file, we will be scraping PSU football roster information. The root URL for the web pages 
             we'll be scraping is https://gopsusports.com/sports/football/roster. These postings go back to the 2009 season. 
             So we will be capturing the roster information from 2009 until 2020. After the data has been processed and cleaned, 
             we will write out a script that can be used to populate our information into a neo4j database.
'''


#First, import the libraries we will be using for scraping
from urllib.request import urlopen
from bs4 import BeautifulSoup

#Build the list of URLs we will be scraping
roster_years = [year for year in range(2009,2021)]

url_list = ['https://gopsusports.com/sports/football/roster/' + str(year) for year in roster_years]


#Define the variables we will be using to store our information
state_dict = {'Pa.':'PA', 'Mich.':'MI', 'Ind.':'IN', 'Calif.':'CA', 'Ore.':'OR',
             'Ill.':'IL','Ohio':'OH','W.Va.':'WV','Texas':'TX', 'Conn.':'CT','N.J.':'NJ',
             'N.Y.':'NY', 'La.':'LA', 'Md.':'MD', 'Fla.':'FL', 'Mass.':'MA', 'Iowa':'IA',
             'Va.':'VA', 'N.C.':'NC', 'Wis.':'WI', 'Tenn.':'TN', 'Ga.':'GA', 'Kan.':'KA',
             'Canada':'Canada','Minn.':'MN','Germany':'Germany','Ala.':'AL', 'N.H.':'NH',
             'Pa':'PA', 'Wash.':'WA','Ariz.':'AZ','Del.':'DE', 'D.C.':'DC','Ont.':'ON',
            'Ontario':'ON','Victoria':'Victoria'}

#Schema of this dictionary: year --> player --> attributes
master_roster_dict = {}

failures = []
state_list = []

#Create a dictionry for player and years on roster
years_on_roster = {}


#Build the functions we will need for processing data from the web pages
def get_players_for_year(url):
    '''
    Scrape the url passed into this function.
    Return the rows of the table that include player information and the roster year
    '''
    year = url.split('/')[-1]

    html = urlopen(url)
    soup = BeautifulSoup(html.read(), "lxml")
    roster = soup.find('table', "sidearm-table sidearm-table-grid-template-1 sidearm-table-grid-template-1-breakdown-large")
    players = roster.find_all('tr')[1:]
    return year, players

def build_player_dict_for_season(year, players):
    '''
    Takes in the raw html corresponding to rows of players in html table.
    Returns a clean dictionary of the players for the year in question
    '''
    roster_dict = {}
    
    for player in players:
        name, position, city, state, high_school = "", "", "", "", ""
        raw_attributes = player.find_all('td')
        clean_attributes = [attribute.text.replace('\n', '') for attribute in raw_attributes]
        name = clean_attributes[1]
        position = clean_attributes[2]
        last_col = clean_attributes[-1]
        last_col_list = last_col.split('/')
        raw_home_town = last_col_list[0]
        high_school = last_col_list[-1].strip()
        try:
            city = raw_home_town.split(',')[0].strip()
            state = raw_home_town.split(',')[1].strip()
            state_list.append(state)
            state = state_dict[state.strip()]
        except:
            failures.append(name)
            pass
        roster_dict[name] = {'name':name, 'position':position, 'city':city.strip(), 'state':state, 'highSchool': high_school.strip()}
    return roster_dict

def build_master_player_dict(url_list):
    '''
    Takes the list of urls as a parameter. Uses the functions defined above to retrieve information from pages
    and store in dictionary.
    '''
    for url in url_list:
        #Return year and players
        year, players = get_players_for_year(url)

        #Clean the player rows and return roster dict for year
        yearly_roster = build_player_dict_for_season(year, players)

        #Add to master_roster_dict with key = year
        master_roster_dict[year] = yearly_roster


build_master_player_dict(url_list)

#Create a dictionry for player and years on roster
years_on_roster = {}

def build_years_on_roster(master_roster_dict):
    players_seen = set()
    #Isolate the roster for a given year
    for year in master_roster_dict:
        yearly_roster = master_roster_dict[year]
        #Iterate through the players and add them to years on roster
        #If they're already been seen, add the year to the list
        for player in yearly_roster:
            if player in players_seen:
                years_on_roster[player].append(int(year))
            else:
                years_on_roster[player] = [int(year)]
                players_seen.add(player)

build_years_on_roster(master_roster_dict)


#Now that we have our dictionaries built, we move to create the script to upload the information into neo4j
#Compile creation statements to load players into neo4j
player_dict_to_load = {}
high_schools = set()
high_school_relationships = {}

def compile_player_dict_to_load(master_roster_dict):
    players_seen = set()
    for year in master_roster_dict:
        yearly_roster = master_roster_dict[year]
        #Iterate through the players and add them to years on roster
        #If they're already been seen, add the year to the list
        for player in yearly_roster:
            if player not in players_seen:
                #Assign player to our player dictionary we are going to load to neo4j
                player_dict = yearly_roster[player]
                player_dict_to_load[player] = player_dict
                #Add high school to our high_schools set. We will add these as nodes to neo4j
                high_schools.add(player_dict['highSchool'])
                high_school_relationships[player] = player_dict['highSchool']
                players_seen.add(player)
            else:
                pass
            
compile_player_dict_to_load(master_roster_dict)


import string
def clean_node_text(node):
    '''
    Accepts a string, strips out punctuation marks and white spaces so that it can be denoted
    as node text for upload to neo4j
    '''
    clean_node = node.translate(str.maketrans('', '', string.punctuation))
    clean_node = clean_node.replace(" ", "")
    return clean_node

#In this block we write out the script to be run in our neo4j database environment
upload_failures = []
f = open("neo4j_script.txt", "w")

#Print player node creation script
def print_player_node_creation_script(file):
    print('MATCH (n) DETACH DELETE n;\n')
    file.write('MATCH (n) DETACH DELETE n;\n')
    for player in player_dict_to_load:
        player_years = years_on_roster[player]
        player_attributes = player_dict_to_load[player]
        player_name = player_attributes['name'].replace("'",'')
        node = clean_node_text(player)
        print_out = ""
            #print out what we want for creation of each player node
        print_out = "CREATE (" + node + ":" + "Person:" + "Player " + "{" + "name: " + "'" + player_name + "', "             + "position: " + "'" + player_attributes['position'] + "', " + "homeCity: " + "'" + player_attributes['city'] + "', "             + "homeState: " + "'" + player_attributes['state'] + "', "              + "yearsOnRoster: " + str(player_years) + "})"
        file.write(print_out + '\n')
        
#Print high school node creation script
def print_high_school_node_creation_script(file):
    for school in high_schools:
        node = clean_node_text(school)
        print_out = "CREATE (" + node + ":" + "School:" + "HighSchool " + "{" + "name: " + "'" + school.replace("'",'') + "'})"
        file.write(print_out + '\n')
        
#Print player to high school relationship script
def print_player_high_school_relationship_script(file):
    for idx, player in enumerate(high_school_relationships):
        #Clean the nodes
        player_node = clean_node_text(player)
        school_node = high_school_relationships[player]
        school_node = clean_node_text(school_node)
        #Print line
        if idx == 0:
            file.write("CREATE\n")
        if idx < len(high_school_relationships) - 1:
            print_line = "(" + player_node + ")-[:GRADUATED_FROM]->(" + school_node + "),"
            file.write(print_line + '\n')
        else:
            print_line = "(" + player_node + ")-[:GRADUATED_FROM]->(" + school_node + ")"
            file.write(print_line)

print_player_node_creation_script(f)
print_high_school_node_creation_script(f)
print_player_high_school_relationship_script(f)

f.close()


