from DataTypes import *
import mysql.connector
import numpy as np
import sys

# Bundles together functions for probing a MySQL database to confirm
# whether or not it adheres to specific properties of a logical/relational schema.
# Can be used to verify that a MySQL database correctly implements a design.
class DataModelChecker:

    # Ctor sets the connection details for this model checker
    def __init__( self, host, username, password, database ):
        # TODO: Implement me!
        
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        
        
    def connect_to_database(self):
        
        try:
            
            self.connection = mysql.connector.connect(host = self.host, username = self.username,\
                                                      password = self.password, database = self.database)

            return self.connection.cursor()
        
        except mysql.connector.Error as E:
            
            print("Error Occured While Connecting to Database.  ", E, "/n")
            
        sys.exit("System Shutdown \n")    
        
        return None
            
       
        
    # Predicate function that connects to the database and confirms
    # whether or not a list of attributes is set up as a (super)key for a given table
    # For example, if attributes contains table_name R and attributes [x, y],
    # this function returns True if (x,y) is enforced as a key in R
    # @see Util.Attributes
    # @pre the tables and attributes in attributes must already exist
    
    def confirmSuperkey( self, attributes ):

        if(len(attributes.table_name) == 0 or len(attributes.attributes) == 0):
            
            return True
        
        table_name = attributes.table_name
        all_attributes = ()
        
        if(len(attributes.attributes) > 1):
            all_attributes = ", ".join(tuple(attributes.attributes))
        else:
            all_attributes = attributes.attributes[0]
            
        
        try:
            
            cursor = self.connect_to_database()
            
            
                     
            counter = 0
            
            if(len(attributes.attributes) >  1):
                
                arr = [1 for i in range(len(all_attributes))]
                arr = ",".join(tuple(str(num) for num in arr))
                
            else:
                
                arr = 1
            
            
            cursor.execute(f"SHOW CREATE TABLE {table_name}")
            res = cursor.fetchone()[1]
            
            pks = []
            uks = []
            lines = res.split("\n")
           
            
            for line in lines:
                
                if "PRIMARY KEY" in line:

                    pkc = line.split(" ")
                    
                    pks.append(pkc[4].replace("(", "").replace(")", "").replace("'","").replace("`","").split(","))
                    
                if "UNIQUE KEY" in line:
                    
                    ukc = line.split(" ")
                    
                    uks.append(ukc[4].replace("(", "").replace(")", "").replace("'","").replace("`","").replace(",", ""))      
             
           
            cursor.execute(f"INSERT INTO {table_name}({all_attributes}) \
                             VALUES({arr}), \
                             ({arr}); ")
            
            cursor.fetchall()
            cursor.close()
            
            return False
            
                                      
        except mysql.connector.Error as E:
            
            counter_1 = 0
            counter_2 = 0
            
            for f in range(len(pks[0])):
                
                if(pks[0][f] in all_attributes ):
                    
                    counter_1 += 1

            for f in range(len(uks)):
                
                if(uks[f] in all_attributes):
                    
                    counter_2 += 1
                    
            if (counter_1 == len(pks[0]) or counter_2 > 0):
                
                cursor.close()
                return True
            
            else:
                
                cursor.close()
                return False
            


    # Predicate function that connects to the database and confirms
    # whether or not `referencing_attributes` is set up as a foreign
    # key that reference `referenced_attributes`
    # For example, if referencing_attributes contains table_name R and attributes [x, y]
    # and referenced_attributes contains table_name S and attributes [a, b]
    # this function returns True if (x,y) is enforced as a foreign key that references
    # (a,b) in R
    # @see Util.Attributes
    # @pre the tables and attributes in referencing_attributes and referenced_attributes must already exist
    def confirmForeignKey( self, referencing_attributes, referenced_attributes ):
        # TODO: Implement me!
        

    
        table_name_referencing =  referencing_attributes.table_name
        all_attributes_referencing =  referencing_attributes.attributes
        
        table_name_referenced = referenced_attributes.table_name
        all_attributes_referenced = referenced_attributes.attributes
        temp_data = []
        fks = []
        
        if(len(table_name_referencing) == 0 or len(table_name_referenced) == 0):
            
            return True
        
        
        if(len(all_attributes_referencing) == 0 or len(all_attributes_referenced) == 0):
            
            return True

              
        try:
            
            
            if(len(all_attributes_referencing) > 1):
                
                all_attributes_referencing = ", ".join(tuple(all_attributes_referencing))
                
            else:
                
                all_attributes_referencing = all_attributes_referencing[0]
                
            if(len(all_attributes_referenced) > 1):
                
                all_attributes_referenced = ", ".join(tuple(all_attributes_referenced))
                
            else:
                
                all_attributes_referenced = all_attributes_referenced[0]
            
            
            
            cursor = self.connect_to_database()
            
            
            arr_1 = ()
            arr_2 = ()
            
            if(len(all_attributes_referencing) >  1):
                
                arr_1 = [1 for i in range(len(all_attributes_referencing))]
                arr_1 = ",".join(tuple(str(num) for num in arr_1))
                
            else:
                
                arr_1 = 1
             
            
            
            if(len(all_attributes_referenced) >  1):
                
                arr_2 = [1 for i in range(len(all_attributes_referenced))]
                arr_2 = ",".join(tuple(str(num) for num in arr_2))
                
            else:
                
                arr_2 = 1
                
                
            cursor.execute(f"SHOW CREATE TABLE {table_name_referencing}")
            res = cursor.fetchone()[1]
            

            lines = res.split("\n")
           
            
            for line in lines:
                
                if "FOREIGN KEY" in line:

                    fkc = line.split(" ")
                    fks.append(fkc[6].replace("(", "").replace(")", "").replace("'","").replace("`","").replace(",",""))
                    temp_str = fkc[7].replace("(", "").replace(")", "").replace("'","").replace("`","").replace(",","")
                    
                    if len(temp_str) == 1 and temp_str.isalpha():
                        
                        fks.append(temp_str)
                        temp_str = fkc[8].replace("(", "").replace(")", "").replace("'","").replace("`","").replace(",","")
                        
                    if len(temp_str) == 1 and temp_str.isalpha():
            
                        fks.append(temp_str)

                
            cursor.execute(f"INSERT INTO {table_name_referenced}({all_attributes_referenced}) VALUES({arr_2}); ")
            
            cursor.fetchall()
            

            cursor.execute(f"INSERT INTO {table_name_referencing}({all_attributes_referencing}) VALUES({arr_1}); ")
            
            cursor.fetchall()
            
            i = 0
            
            while(i < len(all_attributes_referenced)):
                
                cursor.execute(f"DELETE FROM {table_name_referenced} \
                                 WHERE {all_attributes_referenced[i]} = 1 ")
                cursor.fetchall()
                i = i + 1
                
            cursor.execute(f" SELECT COUNT(*) \
                              FROM {table_name_referencing} ")
            
            arr_temp = cursor.fetchall()
            count = 1;
            
            if (arr_temp is not None and arr_temp[0] is not None):
                
                count = arr_temp[0][0]
            
            else:
                
                cursor.close()
                return True
            
            if(count == 0 or count == "NULL"):
                
                cursor.close()
                return True
            
            else:
                
                cursor.close()
                return False
                    
        except mysql.connector.Error as E:
            
            fks = ", ".join(tuple(fks))
            
            for f in range(len(fks)):
                if(fks[f] not in all_attributes_referencing):
                    
                    cursor.close()
                    return False
            
            
            for f in range(len(fks)):
                if(all_attributes_referencing[f] != fks[f]):
                    
                    cursor.close()
                    return False
                    
            cursor.close()
            return True  

    

    # Predicate function that connects to the database and confirms
    # whether or not `referencing_attributes` is set up as a foreign key
    # that reference `referenced_attributes` using a specific referential integrity `policy`
    # For example, if referencing_attributes contains table_name R and attributes [x, y]
    # and referenced_attributes contains table_name S and attributes [a, b]
    # this function returns True if (x,y) the provided policy is used to manage that foreign key
    # @see Util.Attributes, Util.RefIntegrityPolicy
    # @pre The foreign key is valid
    # @pre policy must be a valid Util.RefIntegrityPolicy
    def confirmReferentialIntegrity( self, referencing_attributes, referenced_attributes, policy ):
        # TODO: Implement me!
        

        table_name_referencing =  referencing_attributes.table_name
        all_attributes_referencing =  referencing_attributes.attributes
        
        table_name_referenced = referenced_attributes.table_name
        all_attributes_referenced = referenced_attributes.attributes
        
        if(len(table_name_referencing) == 0 or len(table_name_referenced) == 0):
            
            return True
        
        if(len(all_attributes_referencing) == 0 or len(all_attributes_referenced) == 0):
            
            return True
        
        if(len(policy.policy) == 0 or len(policy.operation) == 0):
            
            return True
        

        try:
            
            cursor = self.connect_to_database()
            
            
            attr_1 = ", ".join(tuple(all_attributes_referencing))
            
          
            attr_2 = ", ".join(tuple(all_attributes_referenced))
            
            arr_1 = ()
            arr_2 = ()
            
            if(len(all_attributes_referencing) >  1):
                
                arr_1 = [1 for i in range(len(all_attributes_referencing))]
                arr_1 = ",".join(tuple(str(num) for num in arr_1))
                
            else:
                
                arr_1 = 1
                
            if(len(all_attributes_referenced) >  1):
                
                arr_2 = [1 for i in range(len(all_attributes_referenced))]
                arr_2 = ",".join(tuple(str(num) for num in arr_2))
                
            else:
                
                arr_2 = 1
                
                        
            if(policy.operation == "INSERT" and policy.policy == "CASCADE" ):
                                                      
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                               
                cursor.execute(f"SELECT COUNT(*) FROM {table_name_referencing}")
                
                arr_temp = cursor.fetchall()  
                
                if (arr_temp is not None or arr_temp[0][0] > 0):
                    
                    cursor.close()           
                    return True
                               
                else:
                    
                    cursor.close()           
                    return False
                               
            elif(policy.operation == "DELETE" and policy.policy == "CASCADE" ):
                
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                               
                for i in range(len(all_attributes_referenced)):
                               
                    cursor.execute(f"DELETE FROM {table_name_referenced} \
                                     WHERE {all_attributes_referenced[i]} = 1 ") 
                               
                    cursor.fetchall()
                               
                cursor.execute(f"SELECT COUNT(*) FROM {table_name_referencing}")
                
                arr_temp = cursor.fetchall()   
                
                if (arr_temp is None or arr_temp[0] is None or arr_temp[0][0] == 0):
                    
                    cursor.close()
                    return True
                               
                else:
                    
                    cursor.close()           
                    return False
                               
            elif(policy.operation == "UPDATE" and policy.policy == "CASCADE" ):
                
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                
                for i in range(len(all_attributes_referenced)):
                               
                    cursor.execute(f"UPDATE {table_name_referenced} \
                                     SET {all_attributes_referenced[i]} = 2 \
                                     WHERE {all_attributes_referenced[i]} = 1") 
                               
                    cursor.fetchall()
                
                for i in range(len(all_attributes_referencing)):
                               
                    cursor.execute(f"SELECT {all_attributes_referencing[i]} FROM {table_name_referencing}")
                    
                    arr_temp = cursor.fetchall()
                    
                    if ((arr_temp is not None and arr_temp[0] is not None) and \
                        (arr_temp[0][0] == 2 or arr_temp[0][0] == "2")):
                        
                        cursor.close()
                        return True
                    
                    
                cursor.close()               
                return False
                               
            
            elif(policy.operation == "INSERT" and policy.policy == "REJECT" ):
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                               
                if(len(all_attributes_referenced) >  1):
                
                    arr_2 = [2 for i in range(len(all_attributes_referenced))]
                    arr_2 = ",".join(tuple(str(num) for num in arr_2))
                
                else:
                
                    arr_2 = 2
                               
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                               
                cursor.fetchall()   
                
                cursor.close()
                return False
                               
                               
            elif(policy.operation == "DELETE" and policy.policy == "REJECT" ):
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                
                for i in range(len(all_attributes_referenced)):
                               
                    cursor.execute(f"DELETE FROM {table_name_referenced} \
                                     WHERE {all_attributes_referenced[i]} = 1 ") 
                               
                    cursor.fetchall()   
                
                cursor.close()
                return False
                               

            elif(policy.operation == "UPDATE" and policy.policy == "REJECT" ):
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                
                for i in range(len(all_attributes_referenced)):
                               
                    cursor.execute(f"UPDATE {table_name_referenced} \
                                     SET {all_attributes_referenced[i]} = 2 \
                                     WHERE {all_attributes_referenced[i]} = 1") 
                               
                    cursor.fetchall()
                
                cursor.close()
                return False

                               
            elif(policy.operation == "INSERT" and policy.policy == "SET NULL" ):
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                
                if(len(all_attributes_referenced) >  1):
                
                    arr_2 = [2 for i in range(len(all_attributes_referenced))]
                    arr_2 = ",".join(tuple(str(num) for num in arr_2))

                else:
                
                    arr_2 = 2
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) " )
                               
                cursor.fetchall()
                
                for i in range(len(all_attributes_referenced)):
                               
                    cursor.execute(f"SELECT ({all_attributes_referencing[i]}) \
                                     FROM {table_name_referencing} ")
                    
                    arr_temp = cursor.fetchall()     
                    
                    if(arr_temp is None or arr_temp[0] is None or \
                       arr_temp[0][0] is None or arr_temp[0][0] == "NULL"):
                        
                        cursor.close()
                        return True
                
                cursor.close()               
                return False
                             
            elif(policy.operation == "DELETE" and policy.policy == "SET NULL" ):
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                
                for i in range(len(all_attributes_referenced)):
                               
                    cursor.execute(f"DELETE FROM {table_name_referenced} \
                                     WHERE {all_attributes_referenced[i]} = 1 ")
                               
                    cursor.fetchall()
                               
                for i in range(len(all_attributes_referencing)):   
                               
                    cursor.execute(f"SELECT {all_attributes_referencing[i]} \
                                     FROM {table_name_referencing} ")
                    
                    arr_temp = cursor.fetchall()          
                    
                    if(arr_temp is None or arr_temp[0] is None or \
                       arr_temp[0][0] is None or arr_temp[0][0] == "NULL"):
                        
                        cursor.close()
                        return True
                    
                cursor.close()               
                return False    
                               
            elif(policy.operation == "UPDATE" and policy.policy == "SET NULL" ):
                               
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                
                for i in range(len(all_attributes_referenced)):
                               
                    cursor.execute(f"UPDATE {table_name_referenced} \
                                     SET {all_attributes_referenced[i]} = 2 \
                                     WHERE {all_attributes_referenced[i]} = 1") 
                               
                    cursor.fetchall()
                
                for i in range(len(all_attributes_referencing)):   
                               
                    cursor.execute(f"SELECT {all_attributes_referencing[i]} \
                                     FROM {table_name_referencing} ")
                    
                    arr_temp = cursor.fetchall()
                    
                    if(arr_temp is None or arr_temp[0] is None or \
                       arr_temp[0][0] is None or arr_temp[0][0] == "NULL"):
                        
                        cursor.close()
                        return True
                               
                cursor.close()               
                return False  
                               
                               
        except mysql.connector.Error as E:
            
            cursor.close()
            return True   



    def confirmFunctionalDependency( self, referencing_attributes, referenced_attributes ):
        # TODO: Implement me!
        
        
        table_name_referencing =  referencing_attributes.table_name
        all_attributes_referencing =  referencing_attributes.attributes
        
        table_name_referenced = referenced_attributes.table_name
        all_attributes_referenced = referenced_attributes.attributes
        temp_data = []
        
        counter_1 = 0
        counter_2 = 0     
        
        if(len(table_name_referencing) == 0 or len(table_name_referenced) == 0):
            
            return True
        
        if(len(all_attributes_referencing) == 0 or len(all_attributes_referenced) == 0):
            
            return True

                
        if(len(all_attributes_referencing) > 1):
                
            attr_1 = ", ".join(tuple(all_attributes_referencing))
                
        else:
                
            attr_1 = all_attributes_referencing[0]
                
        if(len(all_attributes_referenced) > 1):
                
            attr_2 = ", ".join(tuple(all_attributes_referenced))
                
        else:
                
            attr_2 = all_attributes_referenced[0]
            
            
            
        try:
            
      
            cursor = self.connect_to_database()   
            
            arr_1 = []
            arr_2 = []
            
            for i in range(1,5):
                
                if(len(all_attributes_referencing) >  1):

                    arr_1 = [i for x in range(len(all_attributes_referencing))]
                    arr_1 = ",".join(tuple(str(num) for num in arr_1))

                else:

                    arr_1 = i


                if(len(all_attributes_referenced) >  1):

                    arr_2 = [i for x in range(len(all_attributes_referenced))]
                    arr_2 = ",".join(tuple(str(num) for num in arr_2))

                else:

                    arr_2 = i        
                    
              
                cursor.execute(f"INSERT INTO {table_name_referenced}({attr_2}) \
                                 VALUES({arr_2}) ")
                cursor.fetchall()
                
                cursor.execute(f"INSERT INTO {table_name_referencing}({attr_1}) \
                                 VALUES({arr_1}) ")
                               
                cursor.fetchall()
                

                cursor.execute(f"SELECT COUNT(*), {attr_1} \
                                 FROM {table_name_referencing}")
                
                arr_temp = cursor.fetchall()
                
                if(arr_temp is not None):
                    
                    counter_1 = arr_temp[0][0]

                cursor.execute(f"SELECT COUNT(*), {attr_2} \
                                 FROM {table_name_referenced} ")
                
                arr_temp = cursor.fetchall()
                
                if(arr_temp is not None):
                    
                    counter_2 = arr_temp[0][0]
                    
                    
            for j in range(len(all_attributes_referenced)):
                for x in range(1,5):
                    cursor.execute(f"DELETE FROM {table_name_referenced} \
                                     WHERE {all_attributes_referenced[j]} = {x} ")                
                
            if (counter_1 == counter_2):
                
                cursor.close()
                return True
            
            else:
                
                cursor.close()
                return False
        
        except mysql.connector.Error as E:
            
            cursor.close()
            return True
            
            
#References:
#https://pynative.com/python-mysql-database-connection/
