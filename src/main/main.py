import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY ,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    with open(file_path,'r') as file:
        data =csv.reader(file)
        next(data)
        for i in data:
            firstname = i[0].strip()
            lastname = i[1].strip()
            if firstname and lastname and len(i)==2:
                
                cursor.execute(
                    '''
                    INSERT INTO users(firstname,lastname )
                    VALUES(?,?)
                    ''',(firstname,lastname)
                )
                


    print("TODO: load_users")


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    
    with open(file_path,'r') as file:
        data = csv.reader(file)
        next(data)
        for i in data:
            phoneNumber = i[0].strip()
            direction = i[3].strip()
            startTime = i[1]
            endTime = i[2]
            userId = i[4]
            if phoneNumber and startTime.isdigit() and endTime.isdigit() and direction and userId.isdigit() and len(i)==5:
                cursor.execute(
                    '''
                    INSERT INTO callLogs (phoneNumber ,startTime,endTime,direction,userId)
                    VALUES(?,?,?,?,?)
                    ''',(i[0],i[1],i[2],i[3],i[4])
                )
            

        

    print("TODO: load_call_logs")


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):

    duration_dict = {}
    cursor.execute('''
                    SELECT userId,startTime,endTime FROM 
                    callLogs
                    ''')
    table1 = cursor.fetchall()
    for row in table1:
        key = row[0]
        value = row[2]-row[1]
        if key in duration_dict:
            duration_dict[key] +=value
        else:
            duration_dict[key] = value

    # =====================================================================================

    numOfCalls_dict = {}
    cursor.execute('''
                SELECT userId,COUNT(*) AS numOfCalls
                FROM 
                callLogs
                GROUP BY userId
                ''')
    table2 = cursor.fetchall()
    for row in table2:
        numOfCalls_dict[row[0]] = row[1]

    # ========================================================================================

    final_lst = []
    for userId in duration_dict.keys():
        final_lst.append([userId,duration_dict[userId]/numOfCalls_dict[userId],numOfCalls_dict[userId]])
    
    # ==========================================================================================

    with open(csv_file_path,'w',newline = '') as file:
        writer = csv.writer(file)
        writer.writerow(['userId,AvgDuration,numCalls'])
        for row in final_lst:
            writer.writerow(row)


        


    print("TODO: write_user_analytics")


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    cursor.execute(
        '''
        SELECT * FROM callLogs 
        ORDER BY userId,startTime;
        '''
    )
    rows = cursor.fetchall()

    with open(csv_file_path,'w',newline = '') as file:
        append = csv.writer(file)
        append.writerow(['phoneNumber','startTime','endTime','direction','userId'])
        for i in rows:
            append.writerow(i)
    


    print("TODO: write_ordered_calls")



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
