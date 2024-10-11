#!/usr/bin/env python3
import psycopg2

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    # userid = "y24s2c9120_lqia0642"
    # passwd = "Yn5tANZK"
    # myHost = "awsprddbs4836.shared.sydney.edu.au"
    database = "project"
    userid = "postgres"
    passwd = "Ql010602"
    myHost = "localhost"


    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        # conn = psycopg2.connect(database=userid,
        #                             user=userid,
        #                             password=passwd,
        #                             host=myHost)
        conn = psycopg2.connect(database=database,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)


    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate staff based on username and password
'''
def checkLogin(login, password):
    # Connect Database
    conn = openConnection()
    if conn is None:
        return None
    
    cursor = conn.cursor()
    try:
        # We use Cursor to help us to serach user infromation
        cursor.execute(
            '''
                SELECT username, firstname, lastname, email
                FROM  administrator
                WHERE username = %s And password = %s
            '''
            ,(login, password)
        )
        user = cursor.fetchall()
        
        # If we can not find userid , return NULL
        if not user:
            return None
        
        print('User: ',user)
        print('user(flatten):', list(user[0]))
        
        user_info = list(user[0])
        return user_info
        
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
        return None
    finally:
        cursor.close()
        conn.close()

'''
List all the associated admissions records in the database by staff
'''
def findAdmissionsByAdmin(login):

    # Connect Database
    conn = openConnection()
    if conn is None:
        return None
    
    cursor = conn.cursor()
    try:
        # We use Cursor to help us to serach user infromation
        cursor.execute(
            '''
                Select A.admissionid AS admission_id, 
                       ATE.admissiontypename AS admission_type,
                       D.deptname AS admission_department ,
		               COALESCE(TO_CHAR(A.dischargedate,'DD-MM-YYYY'),'') as discharge_date,
		               COALESCE(A.fee :: TEXT,'') as fee,
	                   CONCAT(p.firstname,' ' ,p.lastname) AS patient,
	                   COALESCE(A.condition,'') as condition
                FROM admission A
                JOIN patient P ON A.patient = P.patientid
                Join admissiontype ATE ON A.admissiontype = ATE.admissiontypeid 
                JOIN department D ON A.department = D.deptid
                WHERE A.administrator = 'jdoe'
                ORDER BY A.dischargedate DESC NULLS LAST,
		                 CONCAT(p.firstname,' ' ,p.lastname) ASC,
		                 ATE.admissiontypename DESC
            '''
            ,(login,)
        )
        rows = cursor.fetchall()
        
        # If we can not find rows , return NULL
        if not rows:
            return None
        
        print('rows: ',rows)
        # print('user(flatten):', list(user[0]))
        attributes = [attr[0] for attr in cursor.description]
        print(attributes)
        row_to_dict = [dict(zip(attributes,row)) for row in rows]
        print(row_to_dict)
        return row_to_dict
        
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
        return None
    finally:
        cursor.close()
        conn.close()


'''
Find a list of admissions based on the searchString provided as parameter
See assignment description for search specification
'''
def findAdmissionsByCriteria(searchString):

    return


'''
Add a new addmission 
'''
def addAdmission(type, department, patient, condition, admin):
    
    return


'''
Update an existing admission
'''
def updateAdmission(id, type, department, dischargeDate, fee, patient, condition):
    

    return
