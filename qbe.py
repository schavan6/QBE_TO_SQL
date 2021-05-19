import sys
import graphene
import mysql.connector as mysql

#Temporary default values, will be replaced by credentials user provides
gusername = "snc"
gpassword = "snc"
gdatabase = "classroom"

    
class Column(graphene.ObjectType):
    tname = graphene.String()
    cname = graphene.String()
    ctype = graphene.String()

class QBECondition(graphene.InputObjectType):
    tableName = graphene.String()
    columnName = graphene.String()
    tableCardinality = graphene.String()
    expression = graphene.String()

class QBEResult(graphene.ObjectType):
    values = graphene.List(graphene.List(graphene.String))
    query = graphene.String()
    

class Table(graphene.ObjectType):
    tname = graphene.String()

class Queries(graphene.ObjectType):
    tables = graphene.List(Table,username=graphene.String(),password=graphene.String(),database=graphene.String())
    columns = graphene.List(Column,tablename=graphene.String())
    qberesult = graphene.Field(QBEResult, qbeconditions = graphene.List(QBECondition), conditionBox = graphene.String())
    
    #QBE to SQL conversion and databse query
    def resolve_qberesult(self, info, qbeconditions, conditionBox):
        tablenameToCardinality = {}
        columnNameToVariable = {}
        variableToListOfColumns = {}
        ascDescOrders ={};
        colList = []
        resultData = []
        selectPart = "SELECT "
        queryStarted = False
        selectStarted = False
        conditionBoxProcessed = False

        queryPart = " WHERE "

        for condition in qbeconditions: #iterate over expressions user has entered
            print("@@@@@@"+condition.expression+"@@@@@@@@@@@@@@@@")
           
            if len(condition.expression) == 2 and condition.expression == 'P.': #if expression is just P.

                print(condition.expression,'1')
                if selectStarted:
                    selectPart+= ","
                else:
                    selectStarted = True
                selectPart += " "+condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName+ " "
                colList.append(condition.columnName)
                #-------#
                if condition.tableName in tablenameToCardinality:
                    if condition.tableCardinality > tablenameToCardinality[condition.tableName]:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                else:
                    tablenameToCardinality[condition.tableName] = condition.tableCardinality
                #-------#

               
            elif condition.expression.startswith('P.AO'): #if user expression includes ascending order
                print(condition.expression,'2')
                indexInOrder = condition.expression[5:6]#e.g. in P.AO(1), indexInOrder = 1

                ascDescOrders[int(indexInOrder)] = {'dir':'ASC','column': condition.columnName}

                #!!!!!!!!!!!!!!#
                if selectStarted:
                    selectPart+= ","
                else:
                    selectStarted = True
                selectPart += " "+condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName+ " "
                colList.append(condition.columnName)
                #-------#
                if condition.tableName in tablenameToCardinality:
                    if condition.tableCardinality > tablenameToCardinality[condition.tableName]:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                else:
                    tablenameToCardinality[condition.tableName] = condition.tableCardinality
                #-------#

                #!!!!!!!!!!!!!!#

            elif condition.expression.startswith('P.DO'): #if user expression includes descending order
                print(condition.expression,'3')
                indexInOrder = condition.expression[5:6]
                ascDescOrders[int(indexInOrder)]={'dir':'DESC','column': condition.columnName}

                #!!!!!!!!!!!!!!#
               
                if selectStarted:
                    selectPart+= ","
                else:
                    selectStarted = True
                selectPart += " "+condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName+ " "
                colList.append(condition.columnName)
                #-------#
                if condition.tableName in tablenameToCardinality:
                    if condition.tableCardinality > tablenameToCardinality[condition.tableName]:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                else:
                    tablenameToCardinality[condition.tableName] = condition.tableCardinality
                #-------#

               
                #!!!!!!!!!!!!!!#
            elif condition.expression == 'P.AVG' or (condition.expression[:3]=='P._' and conditionBox[:3]=='AVG' and condition.expression[2:] in conditionBox ):
                print(condition.expression,'4')

                if conditionBox[:3]=='AVG':
                    conditionBoxProcessed = True
        
                if selectStarted:
                    selectPart+=","
                else:
                    selectStarted = True
                    print("!!!!!!!!!!REACHED HERE!!!!!!!!!")
                selectPart += " AVG("+ condition.columnName+")"
                print(selectPart)
                colList.append("Average_"+condition.columnName)
                
                if condition.tableName in tablenameToCardinality:
                    if condition.tableCardinality > tablenameToCardinality[condition.tableName]:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                else:
                    tablenameToCardinality[condition.tableName] = condition.tableCardinality
                

            elif condition.expression.startswith('P.'): #for expressions like 'P._X'
                print(condition.expression,'5')
                if selectStarted:
                    selectPart+= ","
                else:
                    selectStarted = True
                selectPart += " "+condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName+" "
                colList.append(condition.columnName)
                #-------#
                if condition.tableName in tablenameToCardinality:
                    if condition.tableCardinality > tablenameToCardinality[condition.tableName]:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                else:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                #-------#
                columnNameToVariable[condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName] = condition.expression[2:]
                if condition.expression[2:] not in variableToListOfColumns:
                    variableToListOfColumns[condition.expression[2:]] = []
                prevColumns = variableToListOfColumns[condition.expression[2:]]
                prevColumns.append(condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName)
                variableToListOfColumns[condition.expression[2:]]= prevColumns

            elif condition.expression[:1] == '_':#for expressions like '_X' i.e. just variables
                print(condition.expression,'6')
                #-------#
                if condition.tableName in tablenameToCardinality:
                    if condition.tableCardinality > tablenameToCardinality[condition.tableName]:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                else:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                #-------#
                columnNameToVariable[condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName] = condition.expression
                if condition.expression not in variableToListOfColumns:
                    variableToListOfColumns[condition.expression] = []
                prevColumns = variableToListOfColumns[condition.expression]
                prevColumns.append(condition.tableName +'_'+condition.tableCardinality + '.'+ condition.columnName)
                variableToListOfColumns[condition.expression]= prevColumns
            else:#this mostly will handle constants entered by user.
                print(condition.expression,'7')
                #-------#
                if condition.tableName in tablenameToCardinality:
                    if condition.tableCardinality > tablenameToCardinality[condition.tableName]:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                else:
                        tablenameToCardinality[condition.tableName] = condition.tableCardinality
                #-------#
                if queryStarted:
                    queryPart += ' AND '
                else:
                    queryStarted = True
                if condition.expression.startswith(('>','<','<=','>=','!=')):
                    queryPart += " "+condition.tableName + '_' + condition.tableCardinality + '.'+ condition.columnName + condition.expression + " "
                else:    
                    queryPart += " "+condition.tableName + '_' + condition.tableCardinality + '.'+ condition.columnName + '='+ condition.expression + " "

        j=0
       
        #below loop generates a string(orderPart) containing 'order by' column names in their correct order
        #e.g after below loop orderPart = "order by name ASC, city DESC"
        #If user hasn't asked for order by any column, 'orderPart' string will retain it's original value and won't be attached to final query
        orderPart = " ORDER BY "
        sortedOrderBy = sorted(ascDescOrders.items())
        i=0
        for key,value in sortedOrderBy:
            print(value)
            orderPart += value['column'] + " " + value['dir']
            if i!= (len(sortedOrderBy) -1):
                orderPart += ', '
            i+=1


        #below loop generates string(tablePart) containing 'from table_name' part of the query along with aliases of the columns
        #E.g. after this loop tablePart string contain something like - 'FROM building buidling_1, room room_1'
        tablePart = " FROM "
        for table in tablenameToCardinality:
            for i in range(1, int(tablenameToCardinality[table])+1):
                tablePart += table +' '+ table+'_'+ str(i)+ " "
                if i != int(tablenameToCardinality[table]):
                    tablePart += ", "
            if(j!=len(tablenameToCardinality)-1):
                tablePart += ", "
            j+=1


        

        if conditionBoxProcessed == False:
            #below loops will generate string (queryPart above) of where conditions
            #E.g. after these loops queryPart='WHERE room_1.cap > 12'

            #if user has entered something in condition box, it is attached to where clause after replacing the variables with actual column names
            if conditionBox != "":
           
                for column, variable in columnNameToVariable.items():
                    if variable in conditionBox:
                        conditionBox = conditionBox.replace(variable," "+ column+" ")
                if queryStarted:
                    queryPart += ' AND '
                else:
                    queryStarted = True
                queryPart += conditionBox


        #join conditions and constants user entered are attached to where clause (queryPart)
        for variable, columnList in variableToListOfColumns.items():
            if len(columnList) > 1:
                for i in range(0,len(columnList)):
                    if i!= len(columnList)-1:
                        if queryStarted:
                            queryPart += ' AND '
                        else:
                            queryStarted = True
                        queryPart += columnList[i] + '=' + columnList[i+1]


        query = selectPart + tablePart;
        if queryPart != " WHERE ":
            query += queryPart
        if orderPart != " ORDER BY ":
            query += orderPart

        print("QUERY----" + query)

        global gdatabase
        global gusername
        global gpassword
        db = mysql.connect(
            host="localhost",
            database=gdatabase,
            user=gusername,
            password=gpassword,
            auth_plugin='mysql_native_password'
        )

       
        cursor = db.cursor()

        cursor.execute(query)
        records = cursor.fetchall()
           
        resultData.append(colList)
       
        for record in records:
            row = []
            for i in range(0,len(record)):
                row.append(record[i])
            resultData.append(row)      

        cursor.close()
        db.close()

        return QBEResult(values=resultData,query=query)

    #return column info for tables (part 2 of the project)
    def resolve_columns(self, info,tablename):
        global gdatabase
        global gusername
        global gpassword
        db = mysql.connect(
            host="localhost",
            database=gdatabase,
            user=gusername,
            password=gpassword,
            auth_plugin='mysql_native_password'
        )

        columns = []
        cursor = db.cursor()

        #query = "desc "+ tablename
        query = "SELECT column_name, data_type from information_schema.columns where table_name = '"+tablename+ "' and table_schema= '"+gdatabase+"'";
        print("QUERY=== "+ query)
        cursor.execute(query)
        records = cursor.fetchall()
            
        for record in records:
            columns.append(Column(cname=record[0],ctype= record[1],tname = tablename))
        cursor.close()
        db.close()
        return columns

    #Get user credentials and return results
    def resolve_tables(self, info,username,password,database):
        db = mysql.connect(
            host="localhost",
            database=database,
            user=username,
            password=password,
            auth_plugin='mysql_native_password'
        )
        global gusername
        global gpassword
        global gdatabase

        gusername = username
        gpassword = password
        gdatabase = database
        print('get to tables file')
        query = "SELECT table_name FROM information_schema.tables where table_schema = '"+gdatabase+"'";
        cursor = db.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        db.close()
        tables = []
        for record in records:
            tables.append(Table(tname=record[0]))
        return tables



    
schema = graphene.Schema(query=Queries)
