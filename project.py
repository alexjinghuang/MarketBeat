import csv
from langdetect import detect 
import sys

origFile = r'C:\Users\alexz\Documents\AAA_PROXY\Hackathon\GTech2024\orig\data.csv'
cleanFile= r'C:\Users\alexz\Documents\AAA_PROXY\Hackathon\GTech2024\clean.csv'

# Open the CSV file in read mode
with open(origFile, 'r',-1,"utf-8") as file:
    # Create a CSV reader object
    csv_reader = csv.reader(file)
    lineCount = 0
    errorLines = []
    fields = next(csv_reader)
    rows = 650000
    # Iterate over each row in the CSV file
    for i in range(1, rows):
        try:
            lineCount += 1
            var = next(csv_reader)
            stringCheck = (str(var[10]) + " " + str(var[12]))    
            if(detect(stringCheck) != 'en'):
                #print("At line:" + str(lineCount) + " Not english: " + stringCheck + "\n")
                errorLines.append(lineCount)
            
            #print(stringCheck)
            #print(str(lineCount))
        except StopIteration:
            print("No more file")
            break
        except IndexError:
            print("Index error at line: " + str(lineCount))
            sys.exit()
            errorLines.append(lineCount)
        except:
            print("Error at line: " + str(lineCount))
            print("String to check: " + str(stringCheck))
            pass

    print("File processed")
    print(str(errorLines))
    print(len(errorLines))
    print("This many lines precleaning: " + str(lineCount))

    with open(origFile, 'r',-1,"utf-8") as infile, open(cleanFile, 'w',-1,"utf-8", None, "") as cleanFile:
        # creating a csv dict writer object
        origReader = csv.reader(infile)
        writer = csv.writer(cleanFile)
        # writing headers (field names)
        writer.writerow(next(origReader))
        count = 0
        postClean = 0
        for i in range(1, rows):
            try:
                count += 1
                #print(str(lineCount))
                line = next(origReader)
                # Check if the current line should be deleted
                if count not in errorLines:
                    # Write the row to the output CSV file
                    postClean += 1
                    writer.writerow(line)
            except StopIteration:
                break
        print("This many lines after cleaning: " + str(postClean))
        print("Done Cleaning")
