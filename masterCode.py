import json
import os
import csv
import testData  #!!! THIS IS FOR TESTING
# ^^ make SURE you don't put in the 100k list until you feel that your computer can take it lol
import cleanedJobDescription 
import statistics
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
import re
import pandas as pd
from fuzzywuzzy import process, fuzz
from rapidfuzz import fuzz, process

upper_limit = 10
all_data_list = []

""" # This implementation extracts all 10 of the json files and then saves it in a dict 
for x in range(1, upper_limit + 1):

    json_file_path = f'/Users/melaniechen/Desktop/Part 1/cpadataset{x}.json'
    try:
        with open(json_file_path, encoding="utf-8") as f:
            data = json.load(f)
            all_data_list.append(data)
    except Exception as e:
        print(f"Error processing {json_file_path}: {e}")
        break
 """

# This is test implementation for the test json file
# This extracts the one json file and then saves it into a dict 
json_file_path = f'/Users/melaniechen/Desktop/Part 1/practiceDataset.json'
try: 
    with open(json_file_path, encoding="utf-8") as f:
        data = json.load(f)
        all_data_list.append(data)
except Exception as e:
    print (f"Error processing {json_file_path}: {e}")

#All the JSON files in dict form
all_data_dict = {"combined_data": all_data_list}

#The start of the JSON files being passed around
def getProfiles(listOfData):  
    profileList = []
    combinedData = listOfData['combined_data']
    
    for groupOfProfiles in combinedData: 
        for profile in groupOfProfiles: 
            if profile['position'] is not None:
                profileList.append(profile)
                
    return profileList

#This varable contains all the JSON in list form taken from getProfiles (JSON Dict) -> jobPaths -> sortExperience -> transformExperience
profileList = getProfiles(all_data_dict)

#!!! Called by sortExperience
#Transforms experience data from JSON into python dictionary
def transformExperience(experience_data, name):
    count = 0
    title1 = ""
    title2 = ""
  #  print("list of jobs for:  ", name)
    duration = ''

    for item in experience_data:
        count += 1
        if "title" in item:
            json_title = item.get("title", "")
            best_match = process.extractOne(json_title, testData.synonyms, scorer=fuzz.ratio)
            
            if best_match and best_match[1] > 60:   # best_match[1] is the similarity score
                title1 = item["title"]
                item["title"] = best_match[0]      # best_match[0] is the matching title
                title2 = item["title"]
                if 'duration_short' in item:
                    duration = item["duration_short"]
                   # print(title1, " before update job ", count, "  ", title2, " after update job ", count, "   duration: ", duration)

    return experience_data

#THESE 3 FUNCTIONS BELOW DEALS WITH THE DATE ISSUE
def getMonth(string_date):
    numMonths = 6 #Default month is 6 to represent the month of June
    
    if len(string_date) == 2:
        try:
            numMonths = datetime.strptime(string_date[0], "%b").month
            return numMonths / 12
        except ValueError as e:
            print(f"Error converting date" )
            return None
        
      #Returns month divided by 12 to scale it in of years. (1 month is 1/12 of a year)
    return numMonths/12

def getYear(string_date):
    try:
        if len(string_date) == 2:
            year = int(string_date[1])
        elif len(string_date) == 1:
            year = int(string_date[0])
        else:
            print("Invalid string_date format")
            year = 0  #Or another appropriate default value
    except ValueError as e:
        print(f"Error converting to integer: {e}")
        year = 0  #Or another appropriate default value

    return year

def getTotalMonths(duration):
    totalMonths = 0

    if len(duration) == 2 and duration[1] == 'months':
        totalMonths = int(duration[0])
    if len(duration) >= 2 and duration[1] in ['years', 'year']:
        totalMonths = int(duration[0]) * 12
    else:
         print("Invalid duration format")
         
    if len(duration) == 4:
        if duration[0] == 'less':
            totalMonths = 6
        else:
            try:
                totalMonths = int(duration[0]) * 12 + int(duration[2])
            except ValueError:
                    print("Invalid duration format. Unable to convert to months.")
    
    return totalMonths

def sortExperience(experience_data, name):
    experience = []  #Every time LinkedIn updates, it sometimes adds many duplicates, so I use a set to get each unique experience

    try:
        experience_list = json.loads(experience_data)
        #Process the experience_list as needed
    except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON data: {e}")
        print("Skipping this JSON string.")
        return None 

    for entry in experience_list:
        #Checks if "positions" key exists and is a list
        if "positions" in entry:
            #Moves "positions" key contents to the top level
            positions = entry.pop("positions")
            for position in positions:
                experience_list.append(position.copy())

    data_list = transformExperience(experience_list, name)

    #Define the regex pattern
    pattern = re.compile(r'^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{4}$|^\d{4}$')

    #Create a set of English month abbreviations
    english_month_abbreviations = {'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'}

    #Loops through data_list and validate start_date
    for x in data_list:
        start_date = x.get('start_date')
        if start_date is not None:
            #Checks if the start_date matches the pattern
            if not re.match(pattern, start_date):
                return None
            
            #Extracts the month abbreviation
            month_abbreviation = start_date.split()[0]
            
            #Checks if the extracted month abbreviation is not in the set of English month abbreviations
            if month_abbreviation not in english_month_abbreviations:
                return None

    custom_date_format = "%b %Y"  #Assuming 'cze 2016' represents a month abbreviation and a year
    default_date_format = "%Y"    #Default format for year-only dates

    month_mapping = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
    
    #Sorts the data based on the 'start_date' field using the custom date format
    def parse_date(date_str, custom_format, default_format):
        parts = date_str.lower().split() #Splits the date string into parts and convert to lowercase
        
        if len(parts) == 2: #Checks if the date string contains both month abbreviation and year
            month = month_mapping.get(parts[0]) #Gets the numeric value of the month abbreviation
            
            #If month abbreviation is found in the mapping
            if month:  
                return datetime(int(parts[1]), month, 1) #Construct datetime object using year and month
        try:
            return datetime.strptime(date_str, default_format) #Parse using default format
        except ValueError:
            raise ValueError(f"Cannot parse date: {date_str}")
        
    sorted_experience = sorted(
        (x for x in data_list if x.get('start_date') is not None),
        key=lambda x: parse_date(x['start_date'], custom_date_format, default_date_format)
    )

    for i in range(1, len(sorted_experience)):
        if 'end_date' not in sorted_experience[i - 1] or 'start_date' not in sorted_experience[i]:
            continue
        else:
            end_date_1 = sorted_experience[i - 1]['end_date']
            start_date_2 = sorted_experience[i]['start_date']

        if end_date_1 and start_date_2:
            #Convert date strings to datetime objects
            try:
                if end_date_1 == 'Present':
                    end_date_1 = datetime.now()
                else:
                    end_date_1 = datetime.strptime(end_date_1, "%b %Y")
            except ValueError:
                #Handles the case where end_date_1 is only a year or another invalid format
                try:
                    end_date_1 = datetime.strptime(end_date_1, "%Y")
                except ValueError:
                    print("Invalid end_date_1 format:", end_date_1)
                    end_date_1 = None

            try:
                start_date_2 = datetime.strptime(start_date_2, "%b %Y")
            except ValueError:
                #Handles the case where start_date_2 is only a year or another invalid format
                try:
                    start_date_2 = datetime.strptime(start_date_2, "%Y")
                except ValueError:
                    print("Invalid start_date_2 format:", start_date_2)
                    start_date_2 = None

            #Checks if both dates are valid datetime objects before comparison
            if end_date_1 and start_date_2:
                if start_date_2 <= end_date_1:
                    pass
                else:
                    print("start_date_2 is after end_date_1")
         
    for item in sorted_experience:
        if 'duration_short' in item:
            duration_short_value = item['duration_short']
            
            if duration_short_value is not None:
                duration_parts = duration_short_value.split()
                
                if 'start_date' in item and item['start_date'] is not None:
                    string_date = item['start_date'].split()
                    year = getYear(string_date) #Multiplies the year value by 100
                    month = getMonth(string_date)
                    totalMonths = getTotalMonths(duration_parts) #Calculates total months by extracting months and years value from duration
                    
                    if totalMonths >= 0 and 'title' in item:
                    #Handles the case where month or year might be 'None'
                        if month is not None and year is not None:
                            experience.append((f"{month}{year}", totalMonths, item['title']))
                #Rest of the code for processing string_date
                else:
                    #Handle the case where 'start_date' is not present or is None
                    print("Warning: 'start_date' is missing or None for an item.")

    return experience

#This function -> sortExperience -> transformExperience
def jobPaths(profileList):
    jobPathList = []
    
    for profile in profileList:
        experience_data = profile.get('experience', [])
        name = profile.get('name')
        experience = sortExperience(experience_data, name)
        
        if experience:
            experience.append((None, None, profile['position'])) #Current position is None for the first 2 tuple fields so it just appends to the end of the experience array
            jobPathList.append(experience)
            
    return jobPathList

#This vairable taken from jobPaths (json files) -> sortExperience -> transformExperience
jobPathList = jobPaths(profileList)

def calculateDistanceBetweenJobs(currentMonth, initialJob, jobPair, profile, jobMappings):
    terminalJob = ''
    
    for i in range(len(profile)): #Loops through all jobs that have been held by this profile
        j = i + 1

        if j == len(profile):
             break
         
        if len(profile[i]) < 3 or len(profile[j]) < 3:
            continue  #Skip incomplete entries

        initialJob = profile[i][2]
        terminalJob = profile[j][2]
        initialJobDuration = profile[i][1]

        #Verify at this point because if condition isn't passed, months2 += profile[j][1] will have occured but the difference isn't calculated this time, which may affect calculations
        if initialJob != terminalJob and (initialJob, terminalJob) not in jobPair: #There cant be duplicate job mappings within a single person's career path, at least they won't be counted
            jobPair.add((initialJob, terminalJob))
            difference = initialJobDuration
        #    print("first job is: ", initialJob, "  second job is: ", terminalJob)
        #  print("distance between job1 and job2:  ",difference)

            if (initialJob, terminalJob) in jobMappings:
                jobMappings[(initialJob, terminalJob)].append(difference)
            else:
                jobMappings[(initialJob, terminalJob)] = [difference]

#A dictionary of paths from one job to another and an array of months for that mapping the format is like (startJob, endJob):[months]
def createJobMappings(jobPathList):
    jobMappings = {}
    titleMappings = {}
    
    for person in jobPathList:
        months1 = 0
        initialJob = ''
        pair = set()

        #!!! I removed one parameter here
        calculateDistanceBetweenJobs(months1, initialJob, pair, person, jobMappings)

    return jobMappings, titleMappings
      
#!!! THIS FUNCTION DETERMINES THE SIZE OF THE FINAL CSV FILES
def getJobs1():
   # return cleanedJobDescription.job_descriptions #!!! The proper 100k size
    return testData.uniqueTitles #This is the smaller list (1k) to test with

#!!! PROPER IMPLEMENTATION OF OUTPUTING THE CSV FILES
#The the format of the CSV files are being outputted correctly (100k x 100k)
def convert_to_csv_direct(jobs, measure, mappings, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([''] + jobs) #Header row
        
        for job1 in jobs:
            row = [job1]
            for job2 in jobs:
                if job1 == job2:
                    row.append(0)
                else:
                    if (job1, job2) in mappings:
                        values = mappings[(job1, job2)]
                        if measure == 'mean':
                            row.append(int(statistics.mean(values)))
                        elif measure == 'median':
                            row.append(int(statistics.median(values)))
                        elif measure == 'max':
                            row.append(max(values))
                        elif measure == 'min':
                            row.append(min(values))
                        elif measure == 'count':
                            row.append(len(values))
                    else:
                        row.append(0)
            writer.writerow(row)

job_descriptions = getJobs1() #!!! This variable will determine how big the CSV files will be, the 100k x 100k
mappings, titleMap = createJobMappings(jobPathList) #!!! titleMap here may not be needed

# Convert and save to CSV, 
for measure in ['mean', 'median', 'max', 'min', 'count']:  #'mean', 'median', 'max', 'min', 'count'
    convert_to_csv_direct(job_descriptions, measure, mappings, f'{measure}.csv')    
    
print("CSV file is complete")



# #!!! Original transformExperience usin fuzzy wuzzy 
# #Transforms experience data from JSON into python dictionary
# def transformExperience(experience_data, name):
#     count = 0
#     title1 = ""
#     title2 = ""
#     print( "list of jobs for:  ", name)
#     duration = ''

#     for item in experience_data:
#         count += 1
#         if "title" in item:
#             json_title = item.get("title", "")
#             max_similarity = 0
#             matching_title = ""

#             #!!!synonyms.unique_job_titles for quick testing / cleanedJobDescription.job_descriptions for the proper code
#             for original_title in synonyms.unique_job_titles:
#                 # Calculate similarity between json_title and the value in the list
#                 similarity = fuzz.ratio(json_title.lower(), str(original_title).lower())
#                 # Check if the current similarity is higher than the previous maximum
#                 if similarity > max_similarity:
#                     max_similarity = similarity
#                     matching_title = original_title
            
#             matching_title #!!! NOT NECESSARY?

#             # Check if similarity is above a certain threshold (e.g., 60%)
#             if max_similarity > 60:                                                                                                             
#                 title1 = item["title"]
#                # print(item["title"], "   before update ", count)
#                 item["title"] = matching_title
#                 title2 = item["title"]
#                 if 'duration_short' in item:
#                     duration = item["duration_short"]
#                # print(item["title"], "   after update", count)
#                     print(title1, " before update job ", count, "  ", title2, " after update job ", count, "   duration: ", duration)

#     return experience_data